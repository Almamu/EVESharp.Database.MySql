// Copyright (c) 2004, 2022, Oracle and/or its affiliates.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License, version 2.0, as
// published by the Free Software Foundation.
//
// This program is also distributed with certain software (including
// but not limited to OpenSSL) that is licensed under separate terms,
// as designated in a particular file or component or in included license
// documentation.  The authors of MySQL hereby grant you an
// additional permission to link the program and your derivative works
// with the separately licensed software that they have included with
// MySQL.
//
// Without limiting anything contained in the foregoing, this file,
// which is part of MySQL Connector/NET, is also subject to the
// Universal FOSS Exception, version 1.0, a copy of which can be found at
// http://oss.oracle.com/licenses/universal-foss-exception.
//
// This program is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
// See the GNU General Public License, version 2.0, for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software Foundation, Inc.,
// 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

using EVESharp.Database.MySql.Common;
using EVESharp.Database.MySql.Failover;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using EVESharp.Database.MySql;

namespace EVESharp.Database.MySql;

/// <summary>
/// Summary description for MySqlPool.
/// </summary>
internal sealed class MySqlPool
{
    private readonly List <Driver>       _inUsePool;
    private readonly LinkedList <Driver> _idlePool;
    private readonly uint                _minSize;
    private readonly uint                _maxSize;
    private readonly AutoResetEvent      _autoEvent;
    private          int                 _available;
    // Object used to lock the list of host obtained from DNS SRV lookup.
    private readonly object _dnsSrvLock = new object ();

    private void EnqueueIdle (Driver driver)
    {
        driver.IdleSince = DateTime.Now;
        this._idlePool.AddLast (driver);
    }

    public MySqlPool (MySqlConnectionStringBuilder settings)
    {
        this._minSize = settings.MinimumPoolSize;
        this._maxSize = settings.MaximumPoolSize;

        this._available = (int) this._maxSize;
        this._autoEvent = new AutoResetEvent (false);

        if (this._minSize > this._maxSize)
            this._minSize = this._maxSize;

        this.Settings   = settings;
        this._inUsePool = new List <Driver> ((int) this._maxSize);
        this._idlePool  = new LinkedList <Driver> ();

        // prepopulate the idle pool to minSize
        for (int i = 0; i < this._minSize; i++)
            this.EnqueueIdle (this.CreateNewPooledConnection ());

        this.ProcedureCache = new ProcedureCache ((int) settings.ProcedureCacheSize);
    }

#region Properties

    public MySqlConnectionStringBuilder Settings { get; set; }

    public ProcedureCache ProcedureCache { get; }

    /// <summary>
    /// It is assumed that this property will only be used from inside an active
    /// lock.
    /// </summary>
    private bool HasIdleConnections => this._idlePool.Count > 0;

    private int NumConnections => this._idlePool.Count + this._inUsePool.Count;

    /// <summary>
    /// Indicates whether this pool is being cleared.
    /// </summary>
    public bool BeingCleared { get; private set; }

    internal Dictionary <string, string> ServerProperties { get; set; }

#endregion

    /// <summary>
    /// It is assumed that this method is only called from inside an active lock.
    /// </summary>
    private Driver GetPooledConnection ()
    {
        Driver driver = null;

        // if we don't have an idle connection but we have room for a new
        // one, then create it here.
        lock ((this._idlePool as ICollection).SyncRoot)
        {
            if (this.HasIdleConnections)
            {
                driver = this._idlePool.Last.Value;
                this._idlePool.RemoveLast ();
            }
        }

        // Obey the connection timeout
        if (driver != null)
            try
            {
                driver.ResetTimeout ((int) this.Settings.ConnectionTimeout * 1000);
            }
            catch (Exception)
            {
                driver.Close ();
                driver = null;
            }

        if (driver != null)
        {
            // first check to see that the server is still alive
            if (!driver.Ping ())
            {
                driver.Close ();
                driver = null;
            }
            else if (this.Settings.ConnectionReset)
            {
                // if the user asks us to ping/reset pooled connections
                // do so now
                try
                {
                    driver.Reset ();
                }
                catch (Exception)
                {
                    this.Clear ();
                }
            }
        }

        if (driver == null)
            driver = this.CreateNewPooledConnection ();

        Debug.Assert (driver != null);

        lock ((this._inUsePool as ICollection).SyncRoot)
        {
            this._inUsePool.Add (driver);
        }

        return driver;
    }

    /// <summary>
    /// It is assumed that this method is only called from inside an active lock.
    /// </summary>
    private Driver CreateNewPooledConnection ()
    {
        Debug.Assert (this._maxSize - this.NumConnections > 0, "Pool out of sync.");

        Driver driver = Driver.Create (this.Settings);
        driver.Pool = this;
        return driver;
    }

    public void ReleaseConnection (Driver driver)
    {
        lock ((this._inUsePool as ICollection).SyncRoot)
        {
            if (this._inUsePool.Contains (driver))
                this._inUsePool.Remove (driver);
        }

        if (driver.ConnectionLifetimeExpired () || this.BeingCleared)
        {
            driver.Close ();
            Debug.Assert (!this._idlePool.Contains (driver));
        }
        else
        {
            lock ((this._idlePool as ICollection).SyncRoot)
            {
                this.EnqueueIdle (driver);
            }
        }

        lock (this._dnsSrvLock)
        {
            if (driver.Settings.DnsSrv)
            {
                List <DnsSrvRecord> dnsSrvRecords = DnsResolver.GetDnsSrvRecords (DnsResolver.ServiceName);

                FailoverManager.SetHostList (
                    dnsSrvRecords.ConvertAll (r => new FailoverServer (r.Target, r.Port, null)),
                    FailoverMethod.Sequential
                );

                foreach (Driver idleConnection in this._idlePool)
                {
                    string idleServer = idleConnection.Settings.Server;

                    if (!FailoverManager.FailoverGroup.Hosts.Exists (h => h.Host == idleServer) && !idleConnection.IsInActiveUse)
                        idleConnection.Close ();
                }
            }
        }

        Interlocked.Increment (ref this._available);
        this._autoEvent.Set ();
    }

    /// <summary>
    /// Removes a connection from the in use pool.  The only situations where this method 
    /// would be called are when a connection that is in use gets some type of fatal exception
    /// or when the connection is being returned to the pool and it's too old to be 
    /// returned.
    /// </summary>
    /// <param name="driver"></param>
    public void RemoveConnection (Driver driver)
    {
        lock ((this._inUsePool as ICollection).SyncRoot)
        {
            if (this._inUsePool.Contains (driver))
            {
                this._inUsePool.Remove (driver);
                Interlocked.Increment (ref this._available);
                this._autoEvent.Set ();
            }
        }

        // if we are being cleared and we are out of connections then have
        // the manager destroy us.
        if (this.BeingCleared && this.NumConnections == 0)
            MySqlPoolManager.RemoveClearedPool (this);
    }

    private Driver TryToGetDriver ()
    {
        int count = Interlocked.Decrement (ref this._available);

        if (count < 0)
        {
            Interlocked.Increment (ref this._available);
            return null;
        }

        try
        {
            Driver driver = this.GetPooledConnection ();
            return driver;
        }
        catch (Exception ex)
        {
            MySqlTrace.LogError (-1, ex.Message);
            Interlocked.Increment (ref this._available);
            throw;
        }
    }

    public Driver GetConnection ()
    {
        int fullTimeOut = (int) this.Settings.ConnectionTimeout * 1000;
        int timeOut     = fullTimeOut;

        DateTime start = DateTime.Now;

        while (timeOut > 0)
        {
            Driver driver = this.TryToGetDriver ();

            if (driver != null)
                return driver;

            // We have no tickets right now, lets wait for one.
            if (!this._autoEvent.WaitOne (timeOut, false))
                break;

            timeOut = fullTimeOut - (int) DateTime.Now.Subtract (start).TotalMilliseconds;
        }

        throw new MySqlException (Resources.TimeoutGettingConnection);
    }

    /// <summary>
    /// Clears this pool of all idle connections and marks this pool and being cleared
    /// so all other connections are closed when they are returned.
    /// </summary>
    internal void Clear ()
    {
        lock ((this._idlePool as ICollection).SyncRoot)
        {
            // first, mark ourselves as being cleared
            this.BeingCleared = true;

            // then we remove all connections sitting in the idle pool
            while (this._idlePool.Count > 0)
            {
                Driver d = this._idlePool.Last.Value;
                d.Close ();
                this._idlePool.RemoveLast ();
            }

            // there is nothing left to do here.  Now we just wait for all
            // in use connections to be returned to the pool.  When they are
            // they will be closed.  When the last one is closed, the pool will
            // be destroyed.
        }
    }

    /// <summary>
    /// Remove expired drivers from the idle pool
    /// </summary>
    /// <returns></returns>
    /// <remarks>
    /// Closing driver is a potentially lengthy operation involving network
    /// IO. Therefore we do not close expired drivers while holding 
    /// idlePool.SyncRoot lock. We just remove the old drivers from the idle
    /// queue and return them to the caller. The caller will need to close 
    /// them (or let GC close them)
    /// </remarks>
    internal List <Driver> RemoveOldIdleConnections ()
    {
        List <Driver> connectionsToClose = new List <Driver> ();
        DateTime      now                = DateTime.Now;

        lock ((this._idlePool as ICollection).SyncRoot)
        {
            while (this._idlePool.Count > this._minSize)
            {
                Driver   iddleConnection = this._idlePool.First.Value;
                DateTime expirationTime  = iddleConnection.IdleSince.Add (new TimeSpan (0, 0, MySqlPoolManager.maxConnectionIdleTime));

                if (expirationTime.CompareTo (now) < 0)
                {
                    connectionsToClose.Add (iddleConnection);
                    this._idlePool.RemoveFirst ();
                }
                else
                {
                    break;
                }
            }
        }

        return connectionsToClose;
    }
}
// Copyright © 2019, Oracle and/or its affiliates. All rights reserved.
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

using EVESharp.Database.MySql;
using System;
using System.Collections.Generic;

namespace EVESharp.Database.MySql.Failover;

/// <summary>
/// Manages the hosts available for client side failover using the Random Failover method.
/// The Random Failover method attempts to connect to the hosts specified in the list randomly until all the hosts have been attempted.
/// </summary>
internal class RandomFailoverGroup : FailoverGroup
{
    /// <summary>
    /// The initial host taken from the list.
    /// </summary>
    private FailoverServer _initialHost;
    /// <summary>
    /// The host for the current connection attempt.
    /// </summary>
    private FailoverServer _currentHost;
    /// <summary>
    /// Random object to get the next host.
    /// </summary>
    private readonly Random rnd = new Random ();

    public RandomFailoverGroup (List <FailoverServer> hosts) : base (hosts) { }

    /// <summary>
    /// Sets the initial active host.
    /// </summary>
    protected internal override void SetInitialActiveServer ()
    {
        if (this.Hosts == null || this.Hosts.Count == 0)
            throw new MySqlException (Resources.Replication_NoAvailableServer);

        int initialIndex = this.rnd.Next (this.Hosts.Count);
        this._initialHost = this.Hosts [initialIndex];

        this._initialHost.IsActive  = true;
        this._initialHost.Attempted = true;
        this._activeHost            = this._initialHost;
        this._currentHost           = this._activeHost;
    }

    /// <summary>
    /// Determines the next host.
    /// </summary>
    /// <returns>A <see cref="FailoverServer"/> object that represents the next available host.</returns>
    protected internal override FailoverServer GetNextHost ()
    {
        if (this.Hosts == null || this.Hosts?.Count == 0)
            throw new MySqlException (Resources.Replication_NoAvailableServer);

        this.Hosts.Find (h => h.Host == this._currentHost.Host && h.Port == this._currentHost.Port).IsActive = false;
        List <FailoverServer> notAttemptedHosts = this.Hosts.FindAll (h => h.Attempted == false);

        if (notAttemptedHosts.Count > 0)
        {
            this._activeHost           = notAttemptedHosts [this.rnd.Next (notAttemptedHosts.Count)];
            this._activeHost.IsActive  = true;
            this._activeHost.Attempted = true;
            this._currentHost          = this._activeHost;
        }
        else
        {
            this._activeHost = this._initialHost;
        }

        return this._activeHost;
    }
}
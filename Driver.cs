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

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Security;
using System.Text;
using EVESharp.Database.MySql.Common;
using EVESharp.Database.MySql.Types;
using System.IO;
using EVESharp.Database.MySql;

namespace EVESharp.Database.MySql;

/// <summary>
/// Summary description for BaseDriver.
/// </summary>
internal partial class Driver : IDisposable
{
    protected Encoding                     encoding;
    protected MySqlConnectionStringBuilder ConnectionString;
    protected DateTime                     creationTime;
    protected string                       serverCharSet;
    protected Dictionary <string, string>  serverProps;
    internal  int                          timeZoneOffset;
    private   bool                         firstResult;
    protected IDriver                      handler;
    internal  MySqlDataReader              reader;
    private   bool                         disposed;

    /// <summary>
    /// For pooled connections, time when the driver was
    /// put into idle queue
    /// </summary>
    public DateTime IdleSince { get; set; }

    public Driver (MySqlConnectionStringBuilder settings)
    {
        this.encoding = Encoding.GetEncoding ("UTF-8");

        if (this.encoding == null)
            throw new MySqlException (Resources.DefaultEncodingNotFound);

        this.ConnectionString       = settings;
        this.serverCharSet          = "utf8";
        this.ConnectionCharSetIndex = -1;
        this.MaxPacketSize          = 1024;
        this.handler                = new NativeDriver (this);
    }

    ~Driver ()
    {
        this.Dispose (false);
    }

#region Properties

    public int ThreadID => this.handler.ThreadId;

    public DBVersion Version => this.handler.Version;

    public MySqlConnectionStringBuilder Settings
    {
        get => this.ConnectionString;
        set => this.ConnectionString = value;
    }
    public Encoding Encoding
    {
        get => this.encoding;
        set => this.encoding = value;
    }

    public MySqlPromotableTransaction currentTransaction { get; set; }

    public bool IsInActiveUse { get; set; }

    public bool IsOpen { get; protected set; }

    public MySqlPool Pool { get; set; }

    public long MaxPacketSize { get; protected set; }

    protected internal int ConnectionCharSetIndex { get; set; }

    protected internal Dictionary <int, string> CharacterSets { get; protected set; }

    public bool SupportsOutputParameters => this.Version.isAtLeast (5, 5, 0);

    public bool SupportsBatch => (this.handler.Flags & ClientFlags.MULTI_STATEMENTS) != 0;

    public bool SupportsConnectAttrs => (this.handler.Flags & ClientFlags.CONNECT_ATTRS) != 0;

    public bool SupportsPasswordExpiration => (this.handler.Flags & ClientFlags.CAN_HANDLE_EXPIRED_PASSWORD) != 0;

    public bool SupportsQueryAttributes => (this.handler.Flags & ClientFlags.CLIENT_QUERY_ATTRIBUTES) != 0;

    public bool IsPasswordExpired { get; internal set; }

#endregion

    public string Property (string key)
    {
        return this.serverProps [key];
    }

    public bool ConnectionLifetimeExpired ()
    {
        TimeSpan ts = DateTime.Now.Subtract (this.creationTime);

        return this.Settings.ConnectionLifeTime != 0 &&
               ts.TotalSeconds > this.Settings.ConnectionLifeTime;
    }

    public static Driver Create (MySqlConnectionStringBuilder settings)
    {
        Driver d = null;

        try
        {
            if (MySqlTrace.QueryAnalysisEnabled || settings.Logging || settings.UseUsageAdvisor)
                d = new TracingDriver (settings);
        }
        catch (TypeInitializationException ex)
        {
            if (!(ex.InnerException is SecurityException))
                throw;
            //Only rethrow if InnerException is not a SecurityException. If it is a SecurityException then 
            //we couldn't initialize MySqlTrace because we don't have unmanaged code permissions. 
        }

        if (d == null)
            d = new Driver (settings);

        //this try was added as suggested fix submitted on MySql Bug 72025, socket connections are left in CLOSE_WAIT status when connector fails to open a new connection.
        //the bug is present when the client try to get more connections that the server support or has configured in the max_connections variable.
        try
        {
            d.Open ();
        }
        catch
        {
            d.Dispose ();
            throw;
        }

        return d;
    }

    public bool HasStatus (ServerStatusFlags flag)
    {
        return (this.handler.ServerStatus & flag) != 0;
    }

    public virtual void Open ()
    {
        int count = 0;

        do
        {
            try
            {
                this.creationTime = DateTime.Now;
                this.handler.Open ();
                this.IsOpen = true;
                break;
            }
            catch (IOException)
            {
                if (count++ >= 5)
                    throw;
            }
        }
        while (true);
    }

    public virtual void Close ()
    {
        this.Dispose ();
    }

    public virtual void Configure (MySqlConnection connection)
    {
        bool firstConfigure = false;

        // if we have not already configured our server variables
        // then do so now
        if (this.serverProps == null)
        {
            firstConfigure = true;

            // if we are in a pool and the user has said it's ok to cache the
            // properties, then grab it from the pool
            try
            {
                if (this.Pool != null && this.Settings.CacheServerProperties)
                {
                    if (this.Pool.ServerProperties == null)
                        this.Pool.ServerProperties = this.LoadServerProperties (connection);

                    this.serverProps = this.Pool.ServerProperties;
                }
                else
                {
                    this.serverProps = this.LoadServerProperties (connection);
                }

                this.LoadCharacterSets (connection);
            }
            catch (MySqlException ex)
            {
                // expired password capability
                if (ex.Number == 1820)
                {
                    this.IsPasswordExpired = true;
                    return;
                }

                throw;
            }
        }

        // if the user has indicated that we are not to reset
        // the connection and this is not our first time through,
        // then we are done.
        if (!this.Settings.ConnectionReset && !firstConfigure)
            return;

        string charSet = this.ConnectionString.CharacterSet;

        if (string.IsNullOrEmpty (charSet))
        {
            if (this.ConnectionCharSetIndex >= 0 && this.CharacterSets.ContainsKey (this.ConnectionCharSetIndex))
                charSet = this.CharacterSets [this.ConnectionCharSetIndex];
            else
                charSet = this.serverCharSet;
        }

        if (this.serverProps.ContainsKey ("max_allowed_packet"))
            this.MaxPacketSize = Convert.ToInt64 (this.serverProps ["max_allowed_packet"]);

        // now tell the server which character set we will send queries in and which charset we
        // want results in
        MySqlCommand charSetCmd = new MySqlCommand (
            "SET character_set_results=NULL",
            connection
        ) {InternallyCreated = true};

        string clientCharSet;
        this.serverProps.TryGetValue ("character_set_client", out clientCharSet);
        string connCharSet;
        this.serverProps.TryGetValue ("character_set_connection", out connCharSet);

        if ((clientCharSet != null && clientCharSet.ToString () != charSet) ||
            (connCharSet != null && connCharSet.ToString () != charSet))
        {
            MySqlCommand setNamesCmd = new MySqlCommand ("SET NAMES " + charSet, connection);
            setNamesCmd.InternallyCreated = true;
            setNamesCmd.ExecuteNonQuery ();
        }

        // sets character_set_results to null to return values in their original character set
        charSetCmd.ExecuteNonQuery ();

        this.Encoding = CharSetMap.GetEncoding (charSet ?? "utf-8");

        this.handler.Configure ();
    }

    /// <summary>
    /// Loads the properties from the connected server into a hashtable
    /// </summary>
    /// <param name="connection"></param>
    /// <returns></returns>
    private Dictionary <string, string> LoadServerProperties (MySqlConnection connection)
    {
        // load server properties
        Dictionary <string, string> hash = new Dictionary <string, string> ();

        MySqlCommand cmd = new MySqlCommand (
            @"SELECT @@max_allowed_packet, @@character_set_client, 
        @@character_set_connection, @@license, @@sql_mode, @@lower_case_table_names, @@autocommit;", connection
        );

        try
        {
            using (MySqlDataReader reader = cmd.ExecuteReader ())
            {
                while (reader.Read ())
                    for (int i = 0; i <= reader.FieldCount - 1; i++)
                    {
                        string key   = reader.GetName (i).Remove (0, 2);
                        string value = reader [i].ToString ();
                        hash [key] = value;
                    }
            }

            // Get time zone offset as numerical value
            this.timeZoneOffset = this.GetTimeZoneOffset (connection);
            return hash;
        }
        catch (Exception ex)
        {
            MySqlTrace.LogError (this.ThreadID, ex.Message);
            throw;
        }
    }

    private int GetTimeZoneOffset (MySqlConnection con)
    {
        MySqlCommand cmd            = new MySqlCommand ("SELECT TIMEDIFF(NOW(), UTC_TIMESTAMP())", con);
        TimeSpan?    timeZoneDiff   = cmd.ExecuteScalar () as TimeSpan?;
        string       timeZoneString = "0:00";

        if (timeZoneDiff.HasValue)
            timeZoneString = timeZoneDiff.ToString ();

        return int.Parse (timeZoneString.Substring (0, timeZoneString.IndexOf (':')), CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// Loads all the current character set names and ids for this server 
    /// into the charSets hashtable
    /// </summary>
    private void LoadCharacterSets (MySqlConnection connection)
    {
        this.serverProps.TryGetValue ("autocommit", out string serverAutocommit);
        MySqlCommand cmd = new MySqlCommand ("SHOW COLLATION WHERE Id IS NOT NULL", connection);

        // now we load all the currently active collations
        try
        {
            using (MySqlDataReader reader = cmd.ExecuteReader ())
            {
                this.CharacterSets = new Dictionary <int, string> ();

                while (reader.Read ())
                    this.CharacterSets [Convert.ToInt32 (reader ["id"], NumberFormatInfo.InvariantInfo)] =
                        reader.GetString (reader.GetOrdinal ("charset"));
            }

            if (Convert.ToInt32 (serverAutocommit) == 0 && this.Version.isAtLeast (8, 0, 0))
            {
                cmd = new MySqlCommand ("commit", connection);
                cmd.ExecuteNonQuery ();
            }
        }
        catch (Exception ex)
        {
            MySqlTrace.LogError (this.ThreadID, ex.Message);
            throw;
        }
    }

    public virtual List <MySqlError> ReportWarnings (MySqlConnection connection)
    {
        List <MySqlError> warnings = new List <MySqlError> ();

        MySqlCommand cmd = new MySqlCommand ("SHOW WARNINGS", connection) {InternallyCreated = true};

        using (MySqlDataReader reader = cmd.ExecuteReader ())
        {
            while (reader.Read ())
                warnings.Add (
                    new MySqlError (
                        reader.GetString (0),
                        reader.GetInt32 (1), reader.GetString (2)
                    )
                );
        }

        MySqlInfoMessageEventArgs args = new MySqlInfoMessageEventArgs ();
        args.errors = warnings.ToArray ();
        connection?.OnInfoMessage (args);
        return warnings;
    }

    public virtual void SendQuery (MySqlPacket p, int paramsPosition)
    {
        this.handler.SendQuery (p, paramsPosition);
        this.firstResult = true;
    }

    public virtual ResultSet NextResult (int statementId, bool force)
    {
        if (!force && !this.firstResult && !this.HasStatus (ServerStatusFlags.AnotherQuery | ServerStatusFlags.MoreResults))
            return null;

        this.firstResult = false;

        int  affectedRows = -1;
        long insertedId   = -1;
        int  fieldCount   = this.GetResult (statementId, ref affectedRows, ref insertedId);

        if (fieldCount == -1)
            return null;

        if (fieldCount > 0)
            return new ResultSet (this, statementId, fieldCount);
        else
            return new ResultSet (affectedRows, insertedId);
    }

    protected virtual int GetResult (int statementId, ref int affectedRows, ref long insertedId)
    {
        return this.handler.GetResult (ref affectedRows, ref insertedId);
    }

    public virtual bool FetchDataRow (int statementId, int columns)
    {
        return this.handler.FetchDataRow (statementId, columns);
    }

    public virtual bool SkipDataRow ()
    {
        return this.FetchDataRow (-1, 0);
    }

    public virtual void ExecuteDirect (string sql)
    {
        MySqlPacket p = new MySqlPacket (this.Encoding);
        p.WriteString (sql);
        this.SendQuery (p, 0);
        this.NextResult (0, false);
    }

    public MySqlField [] GetColumns (int count)
    {
        MySqlField [] fields = new MySqlField[count];

        for (int i = 0; i < count; i++)
            fields [i] = new MySqlField (this);

        this.handler.GetColumnsData (fields);

        return fields;
    }

    public virtual int PrepareStatement (string sql, ref MySqlField [] parameters)
    {
        return this.handler.PrepareStatement (sql, ref parameters);
    }

    public IMySqlValue ReadColumnValue (int index, MySqlField field, IMySqlValue value)
    {
        return this.handler.ReadColumnValue (index, field, value);
    }

    public void SkipColumnValue (IMySqlValue valObject)
    {
        this.handler.SkipColumnValue (valObject);
    }

    public void ResetTimeout (int timeoutMilliseconds)
    {
        this.handler.ResetTimeout (timeoutMilliseconds);
    }

    public bool Ping ()
    {
        return this.handler.Ping ();
    }

    public virtual void SetDatabase (string dbName)
    {
        this.handler.SetDatabase (dbName);
    }

    public virtual void ExecuteStatement (MySqlPacket packetToExecute)
    {
        this.handler.ExecuteStatement (packetToExecute);
    }

    public virtual void CloseStatement (int id)
    {
        this.handler.CloseStatement (id);
    }

    public virtual void Reset ()
    {
        this.handler.Reset ();
    }

    public virtual void CloseQuery (MySqlConnection connection, int statementId)
    {
        if (this.handler.WarningCount > 0)
            this.ReportWarnings (connection);
    }

#region IDisposable Members

    protected virtual void Dispose (bool disposing)
    {
        if (this.disposed)
            return;

        // Avoid cyclic calls to Dispose.
        this.disposed = true;

        try
        {
            this.ResetTimeout (1000);
            this.handler.Close (this.IsOpen);

            // if we are pooling, then release ourselves
            if (this.ConnectionString.Pooling)
                MySqlPoolManager.RemoveConnection (this);
        }
        catch (Exception ex)
        {
            if (disposing)
            {
                MySqlException mysqlEx = ex as MySqlException;

                if (mysqlEx == null)
                    MySqlTrace.LogError (0, ex.GetBaseException ().Message);
                else
                    MySqlTrace.LogError (mysqlEx.Number, ex.GetBaseException ().Message);
            }
        }
        finally
        {
            this.reader = null;
            this.IsOpen = false;
        }
    }

    public void Dispose ()
    {
        this.Dispose (true);
        GC.SuppressFinalize (this);
    }

#endregion
}

internal interface IDriver
{
    int               ThreadId     { get; }
    DBVersion         Version      { get; }
    ServerStatusFlags ServerStatus { get; }
    ClientFlags       Flags        { get; }
    void              Configure ();
    void              Open ();
    void              SendQuery (MySqlPacket packet, int paramsPosition = 0);
    void              Close (bool            isOpen);
    bool              Ping ();
    int               GetResult (ref int            affectedRows, ref long          insertedId);
    bool              FetchDataRow (int             statementId,  int               columns);
    int               PrepareStatement (string      sql,          ref MySqlField [] parameters);
    void              ExecuteStatement (MySqlPacket packet);
    void              CloseStatement (int           statementId);
    void              SetDatabase (string           dbName);
    void              Reset ();
    IMySqlValue       ReadColumnValue (int          index, MySqlField field, IMySqlValue valObject);
    void              SkipColumnValue (IMySqlValue  valueObject);
    void              GetColumnsData (MySqlField [] columns);
    void              ResetTimeout (int             timeout);
    int               WarningCount { get; }
}
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
using EVESharp.Database.MySql.Types;
using System;
using System.Collections;
using System.ComponentModel;
using System.IO;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using EVESharp.Database.MySql.Authentication;
using EVESharp.Database.MySql;

namespace EVESharp.Database.MySql;

/// <summary>
/// Summary description for Driver.
/// </summary>
internal class NativeDriver : IDriver
{
    private   DBVersion                 version;
    private   int                       threadId;
    protected byte []                   encryptionSeed;
    protected ServerStatusFlags         serverStatus;
    protected MySqlStream               stream;
    protected Stream                    baseStream;
    private   BitArray                  nullMap;
    private   MySqlPacket               packet;
    private   ClientFlags               connectionFlags;
    private   Driver                    owner;
    private   int                       warnings;
    private   MySqlAuthenticationPlugin authPlugin;
    private   MyNetworkStream           networkStream;
    // Windows authentication method string, used by the protocol.
    // Also known as "client plugin name".
    private const string AuthenticationWindowsPlugin = "authentication_windows_client";

    // Predefined username for IntegratedSecurity
    private const string AuthenticationWindowsUser = "auth_windows";

    // Regular expression that checks for GUID format 
    private static Regex guidRegex = new Regex (@"(?i)^[0-9A-F]{8}[-](?:[0-9A-F]{4}[-]){3}[0-9A-F]{12}$");

    public NativeDriver (Driver owner)
    {
        this.owner    = owner;
        this.threadId = -1;
    }

    public ClientFlags Flags => this.connectionFlags;

    public int ThreadId => this.threadId;

    public DBVersion Version => this.version;

    public ServerStatusFlags ServerStatus => this.serverStatus;

    public int WarningCount => this.warnings;

    public MySqlPacket Packet => this.packet;

    internal MySqlConnectionStringBuilder Settings => this.owner.Settings;

    internal Encoding Encoding => this.owner.Encoding;

    private void HandleException (MySqlException ex)
    {
        if (ex.IsFatal)
            this.owner.Close ();
    }

    internal void SendPacket (MySqlPacket p)
    {
        this.stream.SendPacket (p);
    }

    internal void SendEmptyPacket ()
    {
        byte [] buffer = new byte[4];
        this.stream.SendEntirePacketDirectly (buffer, 0);
    }

    internal MySqlPacket ReadPacket ()
    {
        return this.packet = this.stream.ReadPacket ();
    }

    internal OkPacket ReadOk (bool read)
    {
        try
        {
            if (read)
                this.packet = this.stream.ReadPacket ();

            byte header = this.packet.ReadByte ();

            if (header != 0)
                throw new MySqlException ("Out of sync with server", true, null);

            OkPacket okPacket = new OkPacket (this.packet);
            this.serverStatus = okPacket.ServerStatusFlags;

            return okPacket;
        }
        catch (MySqlException ex)
        {
            this.HandleException (ex);
            throw;
        }
    }

    /// <summary>
    /// Sets the current database for the this connection
    /// </summary>
    /// <param name="dbName"></param>
    public void SetDatabase (string dbName)
    {
        byte [] dbNameBytes = this.Encoding.GetBytes (dbName);

        this.packet.Clear ();
        this.packet.WriteByte ((byte) DBCmd.INIT_DB);
        this.packet.Write (dbNameBytes);
        this.ExecutePacket (this.packet);

        this.ReadOk (true);
    }

    public void Configure ()
    {
        this.stream.MaxPacketSize = (ulong) this.owner.MaxPacketSize;
        this.stream.Encoding      = this.Encoding;
    }

    public void Open ()
    {
        // connect to one of our specified hosts
        try
        {
            this.baseStream = StreamCreator.GetStream (this.Settings, ref this.networkStream);

            if (this.Settings.IncludeSecurityAsserts)
                MySqlSecurityPermission.CreatePermissionSet (false).Assert ();
        }
        catch (System.Security.SecurityException)
        {
            throw;
        }
        catch (TimeoutException)
        {
            throw;
        }
        catch (AggregateException ae)
        {
            ae.Handle (
                ex =>
                {
                    if (ex is System.Net.Sockets.SocketException)
                        throw new MySqlException (
                            Resources.UnableToConnectToHost,
                            (int) MySqlErrorCode.UnableToConnectToHost, ex
                        );

                    return ex is MySqlException;
                }
            );
        }
        catch (Exception ex)
        {
            throw new MySqlException (
                Resources.UnableToConnectToHost,
                (int) MySqlErrorCode.UnableToConnectToHost, ex
            );
        }

        if (this.baseStream == null)
            throw new MySqlException (
                Resources.UnableToConnectToHost,
                (int) MySqlErrorCode.UnableToConnectToHost
            );

        int maxSinglePacket = 255 * 255 * 255;
        this.stream = new MySqlStream (this.baseStream, this.Encoding, false, this.networkStream?.Socket);

        this.stream.ResetTimeout ((int) this.Settings.ConnectionTimeout * 1000);

        // read off the welcome packet and parse out it's values
        this.packet = this.stream.ReadPacket ();
        int protocol = this.packet.ReadByte ();

        if (protocol != 10)
            throw new MySqlException ("Unsupported protocol version.");

        string versionString = this.packet.ReadString ();
        this.version  = DBVersion.Parse (versionString);
        this.threadId = this.packet.ReadInteger (4);

        byte [] seedPart1 = this.packet.ReadStringAsBytes ();

        maxSinglePacket = 256 * 256 * 256 - 1;

        // read in Server capabilities if they are provided
        ClientFlags serverCaps = 0;

        if (this.packet.HasMoreData)
            serverCaps = (ClientFlags) this.packet.ReadInteger (2);

        /* New protocol with 16 bytes to describe server characteristics */
        this.owner.ConnectionCharSetIndex = (int) this.packet.ReadByte ();

        this.serverStatus = (ServerStatusFlags) this.packet.ReadInteger (2);

        // Since 5.5, high bits of server caps are stored after status.
        // Previously, it was part of reserved always 0x00 13-byte filler.
        uint serverCapsHigh = (uint) this.packet.ReadInteger (2);
        serverCaps |= (ClientFlags) (serverCapsHigh << 16);

        this.packet.Position += 11;
        byte [] seedPart2 = this.packet.ReadStringAsBytes ();
        this.encryptionSeed = new byte[seedPart1.Length + seedPart2.Length];
        seedPart1.CopyTo (this.encryptionSeed, 0);
        seedPart2.CopyTo (this.encryptionSeed, seedPart1.Length);

        string authenticationMethod = this.Settings.DefaultAuthenticationPlugin;

        if (string.IsNullOrWhiteSpace (authenticationMethod))
        {
            if ((serverCaps & ClientFlags.PLUGIN_AUTH) != 0)
                authenticationMethod = this.packet.ReadString ();
            else
                // Some MySql versions like 5.1, don't give name of plugin, default to native password.
                authenticationMethod = "mysql_native_password";
        }

        // based on our settings, set our connection flags
        this.SetConnectionFlags (serverCaps);

        this.packet.Clear ();
        this.packet.WriteInteger ((int) this.connectionFlags, 4);
        this.packet.WriteInteger (maxSinglePacket,            4);
        this.packet.WriteByte (33); //character set utf-8
        this.packet.Write (new byte[23]);

        // Server doesn't support SSL connections
        if ((serverCaps & ClientFlags.SSL) == 0)
        {
            if (this.Settings.SslMode != MySqlSslMode.Disabled && this.Settings.SslMode != MySqlSslMode.Prefered)
                throw new MySqlException (string.Format (Resources.NoServerSSLSupport, this.Settings.Server));
        }
        // Current connection doesn't support SSL connections
        else if ((this.connectionFlags & ClientFlags.SSL) == 0)
        {
            if (this.Settings.SslMode != MySqlSslMode.Disabled && this.Settings.SslMode != MySqlSslMode.Prefered)
                throw new MySqlException (string.Format (Resources.SslNotAllowedForConnectionProtocol, this.Settings.ConnectionProtocol));
        }
        // Server and connection supports SSL connections and Client are requisting a secure connection
        else
        {
            this.stream.SendPacket (this.packet);

            this.stream = new Ssl (
                    this.Settings.Server, this.Settings.SslMode, this.Settings.CertificateFile, this.Settings.CertificateStoreLocation,
                    this.Settings.CertificatePassword, this.Settings.CertificateThumbprint, this.Settings.SslCa, this.Settings.SslCert,
                    this.Settings.SslKey, this.Settings.TlsVersion
                )
                .StartSSL (ref this.baseStream, this.Encoding, this.Settings.ToString ());

            this.packet.Clear ();
            this.packet.WriteInteger ((int) this.connectionFlags, 4);
            this.packet.WriteInteger (maxSinglePacket,            4);
            this.packet.WriteByte (33); //character set utf-8
            this.packet.Write (new byte[23]);
        }

        this.Authenticate (authenticationMethod, false);

        // if we are using compression, then we use our CompressedStream class
        // to hide the ugliness of managing the compression
        if ((this.connectionFlags & ClientFlags.COMPRESS) != 0)
            this.stream = new MySqlStream (this.baseStream, this.Encoding, true, this.networkStream?.Socket);

        // give our stream the server version we are connected to.  
        // We may have some fields that are read differently based 
        // on the version of the server we are connected to.
        this.packet.Version      = this.version;
        this.stream.MaxBlockSize = maxSinglePacket;
    }

#region Authentication

    /// <summary>
    /// Return the appropriate set of connection flags for our
    /// server capabilities and our user requested options.
    /// </summary>
    private void SetConnectionFlags (ClientFlags serverCaps)
    {
        // We always allow multiple result sets
        ClientFlags flags = ClientFlags.MULTI_RESULTS;

        // allow load data local infile
        if (this.Settings.AllowLoadLocalInfile || !string.IsNullOrWhiteSpace (this.Settings.AllowLoadLocalInfileInPath))
            flags |= ClientFlags.LOCAL_FILES;

        if (!this.Settings.UseAffectedRows)
            flags |= ClientFlags.FOUND_ROWS;

        flags |= ClientFlags.PROTOCOL_41;
        // Need this to get server status values
        flags |= ClientFlags.TRANSACTIONS;

        // user allows/disallows batch statements
        if (this.Settings.AllowBatch)
            flags |= ClientFlags.MULTI_STATEMENTS;

        // if the server allows it, tell it that we want long column info
        if ((serverCaps & ClientFlags.LONG_FLAG) != 0)
            flags |= ClientFlags.LONG_FLAG;

        // if the server supports it and it was requested, then turn on compression
        if ((serverCaps & ClientFlags.COMPRESS) != 0 && this.Settings.UseCompression)
            flags |= ClientFlags.COMPRESS;

        flags |= ClientFlags.LONG_PASSWORD; // for long passwords

        // did the user request an interactive session?
        if (this.Settings.InteractiveSession)
            flags |= ClientFlags.INTERACTIVE;

        // if the server allows it and a database was specified, then indicate
        // that we will connect with a database name
        if ((serverCaps & ClientFlags.CONNECT_WITH_DB) != 0 && this.Settings.Database != null && this.Settings.Database.Length > 0)
            flags |= ClientFlags.CONNECT_WITH_DB;

        // if the server is requesting a secure connection, then we oblige
        if ((serverCaps & ClientFlags.SECURE_CONNECTION) != 0)
            flags |= ClientFlags.SECURE_CONNECTION;

        // if the server is capable of SSL and the user is requesting SSL
        if ((serverCaps & ClientFlags.SSL) != 0 && this.Settings.SslMode != MySqlSslMode.Disabled
                                                && this.Settings.ConnectionProtocol != MySqlConnectionProtocol.NamedPipe
                                                && this.Settings.ConnectionProtocol != MySqlConnectionProtocol.SharedMemory)
            flags |= ClientFlags.SSL;

        // if the server supports output parameters, then we do too
        if ((serverCaps & ClientFlags.PS_MULTI_RESULTS) != 0)
            flags |= ClientFlags.PS_MULTI_RESULTS;

        if ((serverCaps & ClientFlags.PLUGIN_AUTH) != 0)
            flags |= ClientFlags.PLUGIN_AUTH;

        // if the server supports connection attributes
        if ((serverCaps & ClientFlags.CONNECT_ATTRS) != 0)
            flags |= ClientFlags.CONNECT_ATTRS;

        if ((serverCaps & ClientFlags.CAN_HANDLE_EXPIRED_PASSWORD) != 0)
            flags |= ClientFlags.CAN_HANDLE_EXPIRED_PASSWORD;

        // if the server supports query attributes
        if ((serverCaps & ClientFlags.CLIENT_QUERY_ATTRIBUTES) != 0)
            flags |= ClientFlags.CLIENT_QUERY_ATTRIBUTES;

        // if the server supports MFA
        if ((serverCaps & ClientFlags.MULTI_FACTOR_AUTHENTICATION) != 0)
            flags |= ClientFlags.MULTI_FACTOR_AUTHENTICATION;

        // need this to get server session trackers
        flags |= ClientFlags.CLIENT_SESSION_TRACK;

        this.connectionFlags = flags;
    }

    public void Authenticate (string authMethod, bool reset)
    {
        if (authMethod != null)
        {
            // Integrated security is a shortcut for windows auth
            if (this.Settings.IntegratedSecurity)
                authMethod = "authentication_windows_client";

            this.authPlugin = MySqlAuthenticationPlugin.GetPlugin (authMethod, this, this.encryptionSeed);
        }

        this.authPlugin.Authenticate (reset);
    }

#endregion

    public void Reset ()
    {
        this.warnings            = 0;
        this.stream.Encoding     = this.Encoding;
        this.stream.SequenceByte = 0;
        this.packet.Clear ();
        this.packet.WriteByte ((byte) DBCmd.CHANGE_USER);
        this.Authenticate (null, true);
    }

    /// <summary>
    /// Query is the method that is called to send all queries to the server
    /// </summary>
    public void SendQuery (MySqlPacket queryPacket, int paramsPosition)
    {
        this.warnings = 0;
        queryPacket.SetByte (4, (byte) DBCmd.QUERY);
        this.ExecutePacket (queryPacket);
        // the server will respond in one of several ways with the first byte indicating
        // the type of response.
        // 0 == ok packet.  This indicates non-select queries
        // 0xff == error packet.  This is handled in stream.OpenPacket
        // > 0 = number of columns in select query
        // We don't actually read the result here since a single query can generate
        // multiple resultsets and we don't want to duplicate code.  See ReadResult
        // Instead we set our internal server status flag to indicate that we have a query waiting.
        // This flag will be maintained by ReadResult
        this.serverStatus |= ServerStatusFlags.AnotherQuery;
    }

    public void Close (bool isOpen)
    {
        try
        {
            if (isOpen)
                try
                {
                    this.packet.Clear ();
                    this.packet.WriteByte ((byte) DBCmd.QUIT);
                    this.ExecutePacket (this.packet);
                }
                catch (Exception ex)
                {
                    MySqlTrace.LogError (this.ThreadId, ex.ToString ());
                    // Eat exception here. We should try to closing 
                    // the stream anyway.
                }

            if (this.stream != null)
                this.stream.Close ();

            this.stream = null;
        }
        catch (Exception)
        {
            // we are just going to eat any exceptions
            // generated here
        }
    }

    public bool Ping ()
    {
        try
        {
            this.packet.Clear ();
            this.packet.WriteByte ((byte) DBCmd.PING);
            this.ExecutePacket (this.packet);
            this.ReadOk (true);
            return true;
        }
        catch (Exception)
        {
            this.owner.Close ();
            return false;
        }
    }

    public int GetResult (ref int affectedRow, ref long insertedId)
    {
        try
        {
            if (this.stream.Socket == null && this.networkStream?.Socket != null)
                this.stream.Socket = this.networkStream.Socket;

            this.packet = this.stream.ReadPacket ();
        }
        catch (TimeoutException)
        {
            // Do not reset serverStatus, allow to reenter, e.g when
            // ResultSet is closed.
            throw;
        }
        catch (Exception)
        {
            this.serverStatus &= ~(ServerStatusFlags.AnotherQuery |
                                   ServerStatusFlags.MoreResults);

            throw;
        }

        int fieldCount = (int) this.packet.ReadFieldLength ();

        if (-1 == fieldCount)
        {
            if (this.Settings.AllowLoadLocalInfile || !string.IsNullOrWhiteSpace (this.Settings.AllowLoadLocalInfileInPath))
            {
                string filename = this.packet.ReadString ();

                if (!this.Settings.AllowLoadLocalInfile)
                    this.ValidateLocalInfileSafePath (filename);

                this.SendFileToServer (filename);

                return this.GetResult (ref affectedRow, ref insertedId);
            }
            else
            {
                this.stream.Close ();

                if (this.Settings.AllowLoadLocalInfile)
                    throw new MySqlException (Resources.LocalInfileDisabled, (int) MySqlErrorCode.LoadInfo);

                throw new MySqlException (Resources.InvalidPathForLoadLocalInfile, (int) MySqlErrorCode.LoadInfo);
            }
        }
        else if (fieldCount == 0)
        {
            // the code to read last packet will set these server status vars 
            // again if necessary.
            this.serverStatus &= ~(ServerStatusFlags.AnotherQuery |
                                   ServerStatusFlags.MoreResults);

            OkPacket okPacket = new OkPacket (this.packet);
            affectedRow       =  (int) okPacket.AffectedRows;
            insertedId        =  okPacket.LastInsertId;
            this.serverStatus =  okPacket.ServerStatusFlags;
            this.warnings     += okPacket.WarningCount;
        }

        return fieldCount;
    }

    /// <summary>
    /// Verify that the file to upload is in a valid directory
    /// according to the safe path entered by a user under
    /// "AllowLoadLocalInfileInPath" connection option.
    /// </summary>
    /// <param name="filePath">File to validate against the safe path.</param>
    private void ValidateLocalInfileSafePath (string filePath)
    {
        if (!Path.GetFullPath (filePath).StartsWith (Path.GetFullPath (this.Settings.AllowLoadLocalInfileInPath)))
        {
            this.stream.Close ();
            throw new MySqlException (Resources.UnsafePathForLoadLocalInfile, (int) MySqlErrorCode.LoadInfo);
        }
    }

    /// <summary>
    /// Sends the specified file to the server. 
    /// This supports the LOAD DATA LOCAL INFILE
    /// </summary>
    /// <param name="filename"></param>
    private void SendFileToServer (string filename)
    {
        byte [] buffer = new byte[8196];

        long len = 0;

        try
        {
            using (FileStream fs = new FileStream (
                       filename, FileMode.Open,
                       FileAccess.Read
                   ))
            {
                len = fs.Length;

                while (len > 0)
                {
                    int count = fs.Read (buffer, 4, (int) (len > 8192 ? 8192 : len));
                    this.stream.SendEntirePacketDirectly (buffer, count);
                    len -= count;
                }

                this.stream.SendEntirePacketDirectly (buffer, 0);
            }
        }
        catch (Exception ex)
        {
            this.stream.Close ();
            throw new MySqlException ("Error during LOAD DATA LOCAL INFILE", ex);
        }
    }

    private void ReadNullMap (int fieldCount)
    {
        // if we are binary, then we need to load in our null bitmap
        this.nullMap = null;
        byte [] nullMapBytes = new byte[(fieldCount + 9) / 8];
        this.packet.ReadByte ();
        this.packet.Read (nullMapBytes, 0, nullMapBytes.Length);
        this.nullMap = new BitArray (nullMapBytes);
    }

    public IMySqlValue ReadColumnValue (int index, MySqlField field, IMySqlValue valObject)
    {
        long length = -1;
        bool isNull;

        if (this.nullMap != null)
        {
            isNull = this.nullMap [index + 2];

            if (!MySqlField.GetIMySqlValue (field.Type).GetType ().Equals (valObject.GetType ()) && !field.IsUnsigned)
                length = this.packet.ReadFieldLength ();
        }
        else
        {
            length = this.packet.ReadFieldLength ();
            isNull = length == -1;
        }

        if (!isNull && valObject.MySqlDbType is MySqlDbType.Guid && !this.Settings.OldGuids &&
            length > 0 && !guidRegex.IsMatch (this.Encoding.GetString (this.packet.Buffer, this.packet.Position, (int) length)))
        {
            field.Type = MySqlDbType.String;
            valObject  = field.GetValueObject ();
        }

        this.packet.Encoding = field.Encoding;
        this.packet.Version  = this.version;
        IMySqlValue val = valObject.ReadValue (this.packet, length, isNull);

        if (val is MySqlDateTime d)
        {
            d.TimezoneOffset = field.driver.timeZoneOffset;
            return d;
        }

        return val;
    }

    public void SkipColumnValue (IMySqlValue valObject)
    {
        int length = -1;

        if (this.nullMap == null)
        {
            length = (int) this.packet.ReadFieldLength ();

            if (length == -1)
                return;
        }

        if (length > -1)
            this.packet.Position += length;
        else
            valObject.SkipValue (this.packet);
    }

    public void GetColumnsData (MySqlField [] columns)
    {
        for (int i = 0; i < columns.Length; i++)
            this.GetColumnData (columns [i]);

        this.ReadEOF ();
    }

    private void GetColumnData (MySqlField field)
    {
        this.stream.Encoding     = this.Encoding;
        this.packet              = this.stream.ReadPacket ();
        field.Encoding           = this.Encoding;
        field.CatalogName        = this.packet.ReadLenString ();
        field.DatabaseName       = this.packet.ReadLenString ();
        field.TableName          = this.packet.ReadLenString ();
        field.RealTableName      = this.packet.ReadLenString ();
        field.ColumnName         = this.packet.ReadLenString ();
        field.OriginalColumnName = this.packet.ReadLenString ();
        this.packet.ReadByte ();
        field.CharacterSetIndex = this.packet.ReadInteger (2);
        field.ColumnLength      = this.packet.ReadInteger (4);
        MySqlDbType type = (MySqlDbType) this.packet.ReadByte ();
        ColumnFlags colFlags;

        if ((this.connectionFlags & ClientFlags.LONG_FLAG) != 0)
            colFlags = (ColumnFlags) this.packet.ReadInteger (2);
        else
            colFlags = (ColumnFlags) this.packet.ReadByte ();

        field.Scale = (byte) this.packet.ReadByte ();

        if (this.packet.HasMoreData)
            this.packet.ReadInteger (2); // reserved

        if (type == MySqlDbType.Decimal || type == MySqlDbType.NewDecimal)
        {
            field.Precision = (colFlags & ColumnFlags.UNSIGNED) != 0 ? (byte) field.ColumnLength : (byte) (field.ColumnLength - 1);

            if (field.Scale != 0)
                field.Precision--;
        }

        field.SetTypeAndFlags (type, colFlags);
    }

    private void ExecutePacket (MySqlPacket packetToExecute)
    {
        try
        {
            this.warnings            = 0;
            this.stream.SequenceByte = 0;
            this.stream.SendPacket (packetToExecute);
        }
        catch (MySqlException ex)
        {
            this.HandleException (ex);
            throw;
        }
    }

    public void ExecuteStatement (MySqlPacket packetToExecute)
    {
        this.warnings = 0;
        packetToExecute.SetByte (4, (byte) DBCmd.EXECUTE);
        this.ExecutePacket (packetToExecute);
        this.serverStatus |= ServerStatusFlags.AnotherQuery;
    }

    private void CheckEOF ()
    {
        if (!this.packet.IsLastPacket)
            throw new MySqlException ("Expected end of data packet");

        this.packet.ReadByte (); // read off the 254

        if (this.packet.HasMoreData)
        {
            this.warnings     += this.packet.ReadInteger (2);
            this.serverStatus =  (ServerStatusFlags) this.packet.ReadInteger (2);

            // if we are at the end of this cursor based resultset, then we remove
            // the last row sent status flag so our next fetch doesn't abort early
            // and we remove this command result from our list of active CommandResult objects.
            //                if ((serverStatus & ServerStatusFlags.LastRowSent) != 0)
            //              {
            //                serverStatus &= ~ServerStatusFlags.LastRowSent;
            //              commandResults.Remove(lastCommandResult);
            //        }
        }
    }

    private void ReadEOF ()
    {
        this.packet = this.stream.ReadPacket ();
        this.CheckEOF ();
    }

    public int PrepareStatement (string sql, ref MySqlField [] parameters)
    {
        //TODO: check this
        //ClearFetchedRow();

        this.packet.Length = sql.Length * 4 + 5;
        byte [] buffer = this.packet.Buffer;
        int     len    = this.Encoding.GetBytes (sql, 0, sql.Length, this.packet.Buffer, 5);
        this.packet.Position = len + 5;
        buffer [4]           = (byte) DBCmd.PREPARE;
        this.ExecutePacket (this.packet);

        this.packet = this.stream.ReadPacket ();

        int marker = this.packet.ReadByte ();

        if (marker != 0)
            throw new MySqlException ("Expected prepared statement marker");

        int statementId = this.packet.ReadInteger (4);
        int numCols     = this.packet.ReadInteger (2);
        int numParams   = this.packet.ReadInteger (2);
        //TODO: find out what this is needed for
        this.packet.ReadInteger (3);

        if (numParams > 0)
        {
            parameters = this.owner.GetColumns (numParams);

            // we set the encoding for each parameter back to our connection encoding
            // since we can't trust what is coming back from the server
            for (int i = 0; i < parameters.Length; i++)
                parameters [i].Encoding = this.Encoding;
        }

        if (numCols > 0)
        {
            while (numCols-- > 0)
                this.packet = this.stream.ReadPacket ();

            //TODO: handle streaming packets
            this.ReadEOF ();
        }

        return statementId;
    }

    //		private void ClearFetchedRow() 
    //		{
    //			if (lastCommandResult == 0) return;

    //TODO
    /*			CommandResult result = (CommandResult)commandResults[lastCommandResult];
                result.ReadRemainingColumns();

                stream.OpenPacket();
                if (! stream.IsLastPacket)
                    throw new MySqlException("Cursor reading out of sync");

                ReadEOF(false);
                lastCommandResult = 0;*/
    //		}

    /// <summary>
    /// FetchDataRow is the method that the data reader calls to see if there is another 
    /// row to fetch.  In the non-prepared mode, it will simply read the next data packet.
    /// In the prepared mode (statementId > 0), it will 
    /// </summary>
    public bool FetchDataRow (int statementId, int columns)
    {
        /*			ClearFetchedRow();
  
                    if (!commandResults.ContainsKey(statementId)) return false;
  
                    if ( (serverStatus & ServerStatusFlags.LastRowSent) != 0)
                        return false;
  
                    stream.StartPacket(9, true);
                    stream.WriteByte((byte)DBCmd.FETCH);
                    stream.WriteInteger(statementId, 4);
                    stream.WriteInteger(1, 4);
                    stream.Flush();
  
                    lastCommandResult = statementId;
                        */
        this.packet = this.stream.ReadPacket ();

        if (this.packet.IsLastPacket)
        {
            this.CheckEOF ();
            return false;
        }

        this.nullMap = null;

        if (statementId > 0)
            this.ReadNullMap (columns);

        return true;
    }

    public void CloseStatement (int statementId)
    {
        this.packet.Clear ();
        this.packet.WriteByte ((byte) DBCmd.CLOSE_STMT);
        this.packet.WriteInteger ((long) statementId, 4);
        this.stream.SequenceByte = 0;
        this.stream.SendPacket (this.packet);
    }

    /// <summary>
    /// Execution timeout, in milliseconds. When the accumulated time for network IO exceeds this value
    /// TimeoutException is thrown. This timeout needs to be reset for every new command
    /// </summary>
    /// 
    public void ResetTimeout (int timeout)
    {
        if (this.stream != null)
            this.stream.ResetTimeout (timeout);
    }

    internal void SetConnectAttrs ()
    {
        // Sets connect attributes
        if ((this.connectionFlags & ClientFlags.CONNECT_ATTRS) != 0)
        {
            string            connectAttrs = string.Empty;
            MySqlConnectAttrs attrs        = new MySqlConnectAttrs ();

            foreach (PropertyInfo property in attrs.GetType ().GetProperties ())
            {
                string    name        = property.Name;
                object [] customAttrs = property.GetCustomAttributes (typeof (DisplayNameAttribute), false);

                if (customAttrs.Length > 0)
                    name = (customAttrs [0] as DisplayNameAttribute).DisplayName;

                string value = (string) property.GetValue (attrs, null);
                connectAttrs += string.Format ("{0}{1}", (char) name.Length,                           name);
                connectAttrs += string.Format ("{0}{1}", (char) Encoding.UTF8.GetBytes (value).Length, value);
            }

            this.packet.WriteLenString (connectAttrs);
        }
    }
}
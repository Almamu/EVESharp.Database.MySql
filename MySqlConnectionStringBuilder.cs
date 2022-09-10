// Copyright (c) 2013, 2022, Oracle and/or its affiliates.
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
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Text.RegularExpressions;
using EVESharp.Database.MySql.Authentication;
using EVESharp.Database.MySql;

namespace EVESharp.Database.MySql;

/// <summary>
/// Aids in the creation of connection strings by exposing the connection options as properties.
/// Contains connection options specific to the Classic protocol.
/// </summary>
public sealed class MySqlConnectionStringBuilder : MySqlBaseConnectionStringBuilder
{
    static MySqlConnectionStringBuilder ()
    {
        // Add options shared between classic and X protocols from base class.
        Options = MySqlBaseConnectionStringBuilder.Options.Clone ();

        // Server options
        Options.Add (
            new MySqlConnectionStringOption (
                "pipe", "pipe name,pipename", typeof (string), "MYSQL", false,
                (msb, sender, value) =>
                {
                    if (!Platform.IsWindows ())
                        throw new PlatformNotSupportedException (string.Format (Resources.OptionNotCurrentlySupported, nameof (PipeName)));
                    else
                        msb.SetValue ("pipe", value);
                },
                (msb, sender) => msb.PipeName
            )
        );

        Options.Add (
            new MySqlConnectionStringOption (
                "compress", "use compression,usecompression", typeof (bool), false, false,
                (msb, sender, value) => { msb.SetValue ("compress", value); }, (msb, sender) => msb.UseCompression
            )
        );

        Options.Add (
            new MySqlConnectionStringOption (
                "allowbatch", "allow batch", typeof (bool), true, false,
                (msb, sender, value) => { msb.SetValue ("allowbatch", value); }, (msb, sender) => msb.AllowBatch
            )
        );

        Options.Add (
            new MySqlConnectionStringOption (
                "logging", null, typeof (bool), false, false,
                (msb, sender, value) => { msb.SetValue ("logging", value); },
                (msb, sender) => msb.Logging
            )
        );

        Options.Add (
            new MySqlConnectionStringOption (
                "sharedmemoryname", "shared memory name", typeof (string), "MYSQL", false,
                (msb, sender, value) =>
                {
                    if (!Platform.IsWindows ())
                        throw new PlatformNotSupportedException (string.Format (Resources.OptionNotCurrentlySupported, nameof (SharedMemoryName)));
                    else
                        msb.SetValue ("sharedmemoryname", value);
                },
                (msb, sender) => msb.SharedMemoryName
            )
        );

        Options.Add (
            new MySqlConnectionStringOption (
                "defaultcommandtimeout", "command timeout,default command timeout", typeof (uint), (uint) 30, false,
                (msb, sender, value) => { msb.SetValue ("defaultcommandtimeout", value); }, (msb, sender) => msb.DefaultCommandTimeout
            )
        );

        Options.Add (
            new MySqlConnectionStringOption (
                "usedefaultcommandtimeoutforef", "use default command timeout for ef", typeof (bool), false, false,
                (msb, sender, value) => { msb.SetValue ("usedefaultcommandtimeoutforef", value); }, (msb, sender) => msb.UseDefaultCommandTimeoutForEF
            )
        );

        Options.Add (
            new MySqlConnectionStringOption (
                "connectiontimeout", "connection timeout,connect timeout", typeof (uint), (uint) 15, false,
                delegate (MySqlConnectionStringBuilder msb, MySqlConnectionStringOption sender, object Value)
                {
                    uint value = (uint) Convert.ChangeType (Value, sender.BaseType);
                    // Timeout in milliseconds should not exceed maximum for 32 bit
                    // signed integer (~24 days). We truncate the value if it exceeds
                    // maximum (MySqlCommand.CommandTimeout uses the same technique
                    uint timeout = Math.Min (value, int.MaxValue / 1000);

                    if (timeout != value)
                        MySqlTrace.LogWarning (
                            -1, "Connection timeout value too large ("
                                + value + " seconds). Changed to max. possible value" +
                                +timeout + " seconds)"
                        );

                    msb.SetValue ("connectiontimeout", timeout);
                },
                (msb, sender) => (uint) msb.values ["connectiontimeout"]
            )
        );

        Options.Add (new MySqlConnectionStringOption ("allowloadlocalinfile", "allow load local infile", typeof (bool), false, false));

        Options.Add (new MySqlConnectionStringOption ("allowloadlocalinfileinpath", "allow load local infile in path", typeof (string), string.Empty, false));

        // Authentication options.
        Options.Add (
            new MySqlConnectionStringOption (
                "persistsecurityinfo", "persist security info", typeof (bool), false, false,
                (msb, sender, value) => { msb.SetValue ("persistsecurityinfo", value); }, (msb, sender) => msb.PersistSecurityInfo
            )
        );

        Options.Add (
            new MySqlConnectionStringOption (
                "integratedsecurity", "integrated security", typeof (bool), false, false,
                delegate (MySqlConnectionStringBuilder msb, MySqlConnectionStringOption sender, object value)
                {
                    if (!Platform.IsWindows ())
                        throw new MySqlException ("IntegratedSecurity is supported on Windows only");
#if !NETFRAMEWORK
          throw new PlatformNotSupportedException(string.Format(Resources.OptionNotCurrentlySupported, nameof(IntegratedSecurity)));
#else
                    msb.SetValue ("Integrated Security", value.ToString ().Equals ("SSPI", StringComparison.OrdinalIgnoreCase) ? true : value);
#endif
                },
                delegate (MySqlConnectionStringBuilder msb, MySqlConnectionStringOption sender)
                {
                    object val = msb.values ["integratedsecurity"];
                    return (bool) val;
                }
            )
        );

        Options.Add (
            new MySqlConnectionStringOption (
                "allowpublickeyretrieval", null, typeof (bool), false, false,
                (msb, sender, value) => { msb.SetValue ("allowpublickeyretrieval", value); }, (msb, sender) => msb.AllowPublicKeyRetrieval
            )
        );

        Options.Add (
            new MySqlConnectionStringOption (
                "defaultauthenticationplugin", null, typeof (string), string.Empty, false,
                (msb, sender, value) =>
                {
                    if (!string.IsNullOrWhiteSpace ((string) value))
                        AuthenticationPluginManager.ValidateAuthenticationPlugin ((string) value);

                    msb.SetValue ("defaultauthenticationplugin", value);
                },
                (msb, sender) => msb.DefaultAuthenticationPlugin
            )
        );

        Options.Add (new MySqlConnectionStringOption ("ociconfigfile", null, typeof (string), string.Empty, false));

        // Other properties.
        Options.Add (
            new MySqlConnectionStringOption (
                "autoenlist", "auto enlist", typeof (bool), true, false,
                (msb, sender, value) => { msb.SetValue ("autoenlist", value); }, (msb, sender) => msb.AutoEnlist
            )
        );

        Options.Add (
            new MySqlConnectionStringOption (
                "includesecurityasserts", "include security asserts", typeof (bool), false, false,
                (msb, sender, value) => { msb.SetValue ("includesecurityasserts", value); }, (msb, sender) => msb.IncludeSecurityAsserts
            )
        );

        Options.Add (
            new MySqlConnectionStringOption (
                "allowzerodatetime", "allow zero datetime", typeof (bool), false, false,
                (msb, sender, value) => { msb.SetValue ("allowzerodatetime", value); }, (msb, sender) => msb.AllowZeroDateTime
            )
        );

        Options.Add (
            new MySqlConnectionStringOption (
                "convertzerodatetime", "convert zero datetime", typeof (bool), false, false,
                (msb, sender, value) => { msb.SetValue ("convertzerodatetime", value); }, (msb, sender) => msb.ConvertZeroDateTime
            )
        );

        Options.Add (
            new MySqlConnectionStringOption (
                "useusageadvisor", "use usage advisor,usage advisor", typeof (bool), false, false,
                (msb, sender, value) => { msb.SetValue ("useusageadvisor", value); },
                (msb, sender) => msb.UseUsageAdvisor
            )
        );

        Options.Add (
            new MySqlConnectionStringOption (
                "procedurecachesize", "procedure cache size,procedure cache,procedurecache", typeof (uint), (uint) 25, false,
                (msb, sender, value) => { msb.SetValue ("procedurecachesize", value); }, (msb, sender) => msb.ProcedureCacheSize
            )
        );

        Options.Add (
            new MySqlConnectionStringOption (
                "useperformancemonitor", "use performance monitor,useperfmon,perfmon", typeof (bool), false, false,
                (msb, sender, value) =>
                {
#if !NETFRAMEWORK
          throw new PlatformNotSupportedException(string.Format(Resources.OptionNotCurrentlySupported, nameof(UsePerformanceMonitor)));
#else
                    msb.SetValue ("useperformancemonitor", value);
#endif
                },
                (msb, sender) => msb.UsePerformanceMonitor
            )
        );

        Options.Add (
            new MySqlConnectionStringOption (
                "respectbinaryflags", "respect binary flags", typeof (bool), true, false,
                (msb, sender, value) => { msb.SetValue ("respectbinaryflags", value); }, (msb, sender) => msb.RespectBinaryFlags
            )
        );

        Options.Add (
            new MySqlConnectionStringOption (
                "treattinyasboolean", "treat tiny as boolean", typeof (bool), true, false,
                (msb, sender, value) => { msb.SetValue ("treattinyasboolean", value); }, (msb, sender) => msb.TreatTinyAsBoolean
            )
        );

        Options.Add (
            new MySqlConnectionStringOption (
                "allowuservariables", "allow user variables", typeof (bool), false, false,
                (msb, sender, value) => { msb.SetValue ("allowuservariables", value); }, (msb, sender) => msb.AllowUserVariables
            )
        );

        Options.Add (
            new MySqlConnectionStringOption (
                "interactivesession", "interactive session,interactive", typeof (bool), false, false,
                (msb, sender, value) => { msb.SetValue ("interactivesession", value); },
                (msb, sender) => msb.InteractiveSession
            )
        );

        Options.Add (
            new MySqlConnectionStringOption (
                "functionsreturnstring", "functions return string", typeof (bool), false, false,
                (msb, sender, value) => { msb.SetValue ("functionsreturnstring", value); }, (msb, sender) => msb.FunctionsReturnString
            )
        );

        Options.Add (
            new MySqlConnectionStringOption (
                "useaffectedrows", "use affected rows", typeof (bool), false, false,
                (msb, sender, value) => { msb.SetValue ("useaffectedrows", value); }, (msb, sender) => msb.UseAffectedRows
            )
        );

        Options.Add (
            new MySqlConnectionStringOption (
                "oldguids", "old guids", typeof (bool), false, false,
                (msb, sender, value) => { msb.SetValue ("oldguids", value); }, (msb, sender) => msb.OldGuids
            )
        );

        Options.Add (
            new MySqlConnectionStringOption (
                "sqlservermode", "sql server mode", typeof (bool), false, false,
                (msb, sender, value) => { msb.SetValue ("sqlservermode", value); }, (msb, sender) => msb.SqlServerMode
            )
        );

        Options.Add (
            new MySqlConnectionStringOption (
                "tablecaching", "table cache,tablecache", typeof (bool), false, false,
                (msb, sender, value) => { msb.SetValue ("tablecaching", value); }, (msb, sender) => msb.TableCaching
            )
        );

        Options.Add (
            new MySqlConnectionStringOption (
                "defaulttablecacheage", "default table cache age", typeof (int), (int) 60, false,
                (msb, sender, value) => { msb.SetValue ("defaulttablecacheage", value); }, (msb, sender) => msb.DefaultTableCacheAge
            )
        );

        Options.Add (
            new MySqlConnectionStringOption (
                "checkparameters", "check parameters", typeof (bool), true, false,
                (msb, sender, value) => { msb.SetValue ("checkparameters", value); }, (msb, sender) => msb.CheckParameters
            )
        );

        Options.Add (
            new MySqlConnectionStringOption (
                "replication", null, typeof (bool), false, false,
                (msb, sender, value) => { msb.SetValue ("replication", value); },
                (msb, sender) => msb.Replication
            )
        );

        Options.Add (
            new MySqlConnectionStringOption (
                "exceptioninterceptors", "exception interceptors", typeof (string), null, false,
                (msb, sender, value) => { msb.SetValue ("exceptioninterceptors", value); }, (msb, sender) => msb.ExceptionInterceptors
            )
        );

        Options.Add (
            new MySqlConnectionStringOption (
                "commandinterceptors", "command interceptors", typeof (string), null, false,
                (msb, sender, value) => { msb.SetValue ("commandinterceptors", value); }, (msb, sender) => msb.CommandInterceptors
            )
        );

        // Pooling options.
        Options.Add (
            new MySqlConnectionStringOption (
                "connectionlifetime", "connection lifetime", typeof (uint), (uint) 0, false,
                (msb, sender, value) => { msb.SetValue ("connectionlifetime", value); }, (msb, sender) => msb.ConnectionLifeTime
            )
        );

        Options.Add (
            new MySqlConnectionStringOption (
                "pooling", null, typeof (bool), true, false,
                (msb, sender, value) => { msb.SetValue ("pooling", value); }, (msb, sender) => msb.Pooling
            )
        );

        Options.Add (
            new MySqlConnectionStringOption (
                "minpoolsize", "minimumpoolsize,min pool size,minimum pool size", typeof (uint), (uint) 0, false,
                (msb, sender, value) => { msb.SetValue ("minpoolsize", value); }, (msb, sender) => msb.MinimumPoolSize
            )
        );

        Options.Add (
            new MySqlConnectionStringOption (
                "maxpoolsize", "maximumpoolsize,max pool size,maximum pool size", typeof (uint), (uint) 100, false,
                (msb, sender, value) => { msb.SetValue ("maxpoolsize", value); }, (msb, sender) => msb.MaximumPoolSize
            )
        );

        Options.Add (
            new MySqlConnectionStringOption (
                "connectionreset", "connection reset", typeof (bool), false, false,
                (msb, sender, value) => { msb.SetValue ("connectionreset", value); }, (msb, sender) => msb.ConnectionReset
            )
        );

        Options.Add (
            new MySqlConnectionStringOption (
                "cacheserverproperties", "cache server properties", typeof (bool), false, false,
                (msb, sender, value) => { msb.SetValue ("cacheserverproperties", value); }, (msb, sender) => msb.CacheServerProperties
            )
        );

        // Language and charset options.
        Options.Add (
            new MySqlConnectionStringOption (
                "treatblobsasutf8", "treat blobs as utf8", typeof (bool), false, false,
                (msb, sender, value) => { msb.SetValue ("treatblobsasutf8", value); }, (msb, sender) => msb.TreatBlobsAsUTF8
            )
        );

        Options.Add (
            new MySqlConnectionStringOption (
                "blobasutf8includepattern", null, typeof (string), "", false,
                (msb, sender, value) => { msb.SetValue ("blobasutf8includepattern", value); }, (msb, sender) => msb.BlobAsUTF8IncludePattern
            )
        );

        Options.Add (
            new MySqlConnectionStringOption (
                "blobasutf8excludepattern", null, typeof (string), "", false,
                (msb, sender, value) => { msb.SetValue ("blobasutf8excludepattern", value); }, (msb, sender) => msb.BlobAsUTF8ExcludePattern
            )
        );
    }

    /// <summary>
    /// Main constructor.
    /// </summary>
    public MySqlConnectionStringBuilder ()
    {
        this.values        = new Dictionary <string, object> ();
        this.HasProcAccess = true;

        // Populate initial values.
        lock (this)
        {
            foreach (MySqlConnectionStringOption option in Options.Options)
                this.values [option.Keyword] = option.DefaultValue;
        }
    }

    /// <summary>
    /// Constructor accepting a connection string.
    /// </summary>
    /// <param name="connectionString">The connection string.</param>
    public MySqlConnectionStringBuilder (string connectionString) : this ()
    {
        this.AnalyzeConnectionString (connectionString, false);

        lock (this)
        {
            this.ConnectionString = connectionString;
        }
    }

    /// <summary>
    /// Readonly field containing a collection of classic protocol and protocol shared connection options.
    /// </summary>
    internal new static readonly MySqlConnectionStringOptionCollection Options;

#region Server Properties

    /// <summary>
    /// Gets or sets the name of the named pipe that should be used
    /// for communicating with MySQL.
    /// </summary>
    /// <remarks>This property has no effect unless the <see cref="MySqlBaseConnectionStringBuilder.ConnectionProtocol"/> 
    /// property has been set to <see cref="MySqlConnectionProtocol.NamedPipe"/>.</remarks>
    [Category ("Connection")]
    [DisplayName ("Pipe Name")]
    [Description ("Name of pipe to use when connecting with named pipes (Win32 only)")]
    [RefreshProperties (RefreshProperties.All)]
    public string PipeName
    {
        get => (string) this.values ["pipe"];
        set => this.SetValue ("pipe", value);
    }

    /// <summary>
    /// Gets or sets a boolean value that indicates whether this connection
    /// should use compression.
    /// </summary>
    [Category ("Connection")]
    [DisplayName ("Use Compression")]
    [Description ("Should the connection use compression")]
    [RefreshProperties (RefreshProperties.All)]
    public bool UseCompression
    {
        get => (bool) this.values ["compress"];
        set => this.SetValue ("compress", value);
    }

    /// <summary>
    /// Gets or sets a boolean value that indicates whether this connection will allow
    /// commands to send multiple SQL statements in one execution.
    /// </summary>
    [Category ("Connection")]
    [DisplayName ("Allow Batch")]
    [Description ("Allows execution of multiple SQL commands in a single statement")]
    [RefreshProperties (RefreshProperties.All)]
    public bool AllowBatch
    {
        get => (bool) this.values ["allowbatch"];
        set => this.SetValue ("allowbatch", value);
    }

    /// <summary>
    /// Gets or sets a boolean value that indicates whether logging is enabled.
    /// </summary>
    [Category ("Connection")]
    [Description ("Enables output of diagnostic messages")]
    [RefreshProperties (RefreshProperties.All)]
    public bool Logging
    {
        get => (bool) this.values ["logging"];
        set => this.SetValue ("logging", value);
    }

    /// <summary>
    /// Gets or sets the base name of the shared memory objects used to 
    /// communicate with MySQL when the shared memory protocol is being used.
    /// </summary>
    [Category ("Connection")]
    [DisplayName ("Shared Memory Name")]
    [Description ("Name of the shared memory object to use")]
    [RefreshProperties (RefreshProperties.All)]
    public string SharedMemoryName
    {
        get => (string) this.values ["sharedmemoryname"];
        set => this.SetValue ("sharedmemoryname", value);
    }

    /// <summary>
    /// Gets or sets the default command timeout.
    /// </summary>
    [Category ("Connection")]
    [DisplayName ("Default Command Timeout")]
    [Description (
        @"The default timeout that MySqlCommand objects will use
                     unless changed."
    )]
    [RefreshProperties (RefreshProperties.All)]
    public uint DefaultCommandTimeout
    {
        get => (uint) this.values ["defaultcommandtimeout"];
        set => this.SetValue ("defaultcommandtimeout", value);
    }

    /// <summary>
    /// Gets or sets the connection timeout.
    /// </summary>
    [Category ("Connection")]
    [DisplayName ("Connect Timeout")]
    [Description (
        "The length of time (in seconds) to wait for a connection " +
        "to the server before terminating the attempt and generating an error."
    )]
    [RefreshProperties (RefreshProperties.All)]
    public uint ConnectionTimeout
    {
        get => (uint) this.values ["connectiontimeout"];

        set
        {
            // Timeout in milliseconds should not exceed maximum for 32 bit
            // signed integer (~24 days). We truncate the value if it exceeds
            // maximum (MySqlCommand.CommandTimeout uses the same technique
            uint timeout = Math.Min (value, int.MaxValue / 1000);

            if (timeout != value)
                MySqlTrace.LogWarning (
                    -1, "Connection timeout value too large ("
                        + value + " seconds). Changed to max. possible value" +
                        +timeout + " seconds)"
                );

            this.SetValue ("connectiontimeout", timeout);
        }
    }

    /// <summary>
    /// Gets or sets a boolean value that indicates whether this connection will allow
    /// to load data local infile.
    /// </summary>
    [Category ("Connection")]
    [DisplayName ("Allow Load Data Local Infile")]
    [Description ("Allows reading data from a text file.")]
    [RefreshProperties (RefreshProperties.All)]
    public bool AllowLoadLocalInfile
    {
        get => (bool) this.values ["allowloadlocalinfile"];
        set => this.SetValue ("allowloadlocalinfile", value);
    }

    /// <summary>
    /// Gets or sets the safe path where files can be read and uploaded to the server.
    /// </summary>
    [Category ("Connection")]
    [DisplayName ("Allow Load Local Infile In Path")]
    [Description ("Allows specifying a safe path to read and upload files to server.")]
    [RefreshProperties (RefreshProperties.All)]
    public string AllowLoadLocalInfileInPath
    {
        get => (string) this.values ["allowloadlocalinfileinpath"];
        set => this.SetValue ("allowloadlocalinfileinpath", value);
    }

#endregion

#region Authentication Properties

    /// <summary>
    /// Gets or sets a boolean value that indicates if the password should be persisted
    /// in the connection string.
    /// </summary>
    [Category ("Security")]
    [DisplayName ("Persist Security Info")]
    [Description (
        "When false, security-sensitive information, such as the password, " +
        "is not returned as part of the connection if the connection is open or " +
        "has ever been in an open state."
    )]
    [RefreshProperties (RefreshProperties.All)]
    public bool PersistSecurityInfo
    {
        get => (bool) this.values ["persistsecurityinfo"];
        set => this.SetValue ("persistsecurityinfo", value);
    }

    /// <summary>
    /// Gets or sets a boolean value that indicates if the connection should be encrypted.
    /// </summary>
    /// <remarks>Obsolte. Use <see cref="MySqlSslMode"/> instead.</remarks>
    [Category ("Authentication")]
    [DisplayName ("Integrated Security")]
    [Description ("Use windows authentication when connecting to server")]
    [DefaultValue (false)]
    public bool IntegratedSecurity
    {
        get => (bool) this.values ["integratedsecurity"];
        set => this.SetValue ("integratedsecurity", value);
    }

    /// <summary>
    /// Gets or sets a boolean value that indicates if RSA public keys should be retrieved from the server.
    /// </summary>
    /// <remarks>This option is only relevant when SSL is disabled. Setting this option to <c>true</c> in
    /// 8.0 servers that have the caching_sha2_password authentication plugin as the default plugin will
    /// cause the connection attempt to fail if the user hasn't successfully connected to the server on a
    /// previous occasion.</remarks>
    [Category ("Authentication")]
    [DisplayName ("AllowPublicKeyRetrieval")]
    [Description ("Allow retrieval of RSA public keys from server when SSL is disabled.")]
    [DefaultValue (false)]
    public bool AllowPublicKeyRetrieval
    {
        get => (bool) this.values ["allowpublickeyretrieval"];
        set => this.SetValue ("allowpublickeyretrieval", value);
    }

    /// <summary>
    /// Gets or sets the default authentication plugin to be used. This plugin takes precedence over
    /// the server-side default authentication plugin when a valid authentication plugin is specified.
    /// </summary>
    /// <remarks>
    /// The default authentication plugin is mandatory for supporting user-less and password-less Kerberos authentications. 
    /// If no value is set, it uses the server-side default authentication plugin.
    /// </remarks>
    [Category ("Authentication")]
    [DisplayName ("DefaultAuthenticationPlugin")]
    [Description (
        "Enables the setting of an authentication plugin that takes precedence over the server-side" +
        "default authentication plugin."
    )]
    public string DefaultAuthenticationPlugin
    {
        get => (string) this.values ["defaultauthenticationplugin"];
        set => this.SetValue ("defaultauthenticationplugin", value);
    }

    /// <summary>
    /// Gets or sets the OCI config file location.
    /// </summary>
    /// <remarks>
    /// The default values vary depending on the OS. On Windows systems the value is '%HOMEDRIVE%%HOMEPATH%\.oci\config' 
    /// and for Linux/MacOS systems it is '~/.oci/config'.
    /// </remarks>
    [Category ("Authentication")]
    [DisplayName ("OciConfigFile")]
    [Description ("Specifies the OCI configuration file location.")]
    public string OciConfigFile
    {
        get => (string) this.values ["ociconfigfile"];
        set => this.SetValue ("ociconfigfile", value);
    }

#endregion

#region Other Properties

    /// <summary>
    /// Gets or sets a boolean value that indicates if zero date time values are supported.
    /// </summary>
    /// <remarks>Default value is <c>false</c>.</remarks>
    [Category ("Advanced")]
    [DisplayName ("Allow Zero Datetime")]
    [Description ("Should zero datetimes be supported")]
    [RefreshProperties (RefreshProperties.All)]
    [DefaultValue (false)]
    public bool AllowZeroDateTime
    {
        get => (bool) this.values ["allowzerodatetime"];
        set => this.SetValue ("allowzerodatetime", value);
    }

    /// <summary>
    /// Gets or sets a boolean value that indicates if zero datetime values should be
    /// converted to DateTime.MinValue.
    /// </summary>
    /// <remarks>Default value is <c>false</c>.</remarks>
    [Category ("Advanced")]
    [DisplayName ("Convert Zero Datetime")]
    [Description ("Should illegal datetime values be converted to DateTime.MinValue")]
    [RefreshProperties (RefreshProperties.All)]
    [DefaultValue (false)]
    public bool ConvertZeroDateTime
    {
        get => (bool) this.values ["convertzerodatetime"];
        set => this.SetValue ("convertzerodatetime", value);
    }

    /// <summary>
    /// Gets or sets a boolean value that indicates if the Usage Advisor should be enabled.
    /// </summary>
    /// <remarks>Default value is <c>false</c>.</remarks>
    [Category ("Advanced")]
    [DisplayName ("Use Usage Advisor")]
    [Description ("Logs inefficient database operations")]
    [RefreshProperties (RefreshProperties.All)]
    [DefaultValue (false)]
    public bool UseUsageAdvisor
    {
        get => (bool) this.values ["useusageadvisor"];
        set => this.SetValue ("useusageadvisor", value);
    }

    /// <summary>
    /// Gets or sets the size of the stored procedure cache.
    /// </summary>
    /// <remarks>Default value is 25.</remarks>
    [Category ("Advanced")]
    [DisplayName ("Procedure Cache Size")]
    [Description (
        "Indicates how many stored procedures can be cached at one time. " +
        "A value of 0 effectively disables the procedure cache."
    )]
    [RefreshProperties (RefreshProperties.All)]
    [DefaultValue (25)]
    public uint ProcedureCacheSize
    {
        get => (uint) this.values ["procedurecachesize"];
        set => this.SetValue ("procedurecachesize", value);
    }

    /// <summary>
    /// Gets or sets a boolean value that indicates if the performance monitor hooks should be enabled.
    /// </summary>
    /// <remarks>Default value is <c>false</c>.</remarks>
    [Category ("Advanced")]
    [DisplayName ("Use Performance Monitor")]
    [Description ("Indicates that performance counters should be updated during execution.")]
    [RefreshProperties (RefreshProperties.All)]
    [DefaultValue (false)]
    public bool UsePerformanceMonitor
    {
        get => (bool) this.values ["useperformancemonitor"];
        set => this.SetValue ("useperformancemonitor", value);
    }

    /// <summary>
    /// Gets or sets a boolean value that indicates if an opened connection should particiapte in the current scope.
    /// </summary>
    /// <remarks>Default value is <c>true</c>.</remarks>
    [Category ("Advanced")]
    [DisplayName ("Auto Enlist")]
    [Description ("Should the connetion automatically enlist in the active connection, if there are any.")]
    [RefreshProperties (RefreshProperties.All)]
    [DefaultValue (true)]
    public bool AutoEnlist
    {
        get => (bool) this.values ["autoenlist"];
        set => this.SetValue ("autoenlist", value);
    }

    /// <summary>
    /// Gets or sets a boolean value that indicates if security asserts must be included.
    /// </summary>
    /// <remarks>Must be set to <c>true</c> when using the <see cref="MySqlClientPermission"/> class in a partial trust environment,
    /// with the library installed in the GAC of the hosting environment. Not supported in .NET Core.
    /// Default value is <c>false</c>.</remarks>
    [Category ("Advanced")]
    [DisplayName ("Include Security Asserts")]
    [Description ("Include security asserts to support Medium Trust")]
    [DefaultValue (false)]
    public bool IncludeSecurityAsserts
    {
        get => (bool) this.values ["includesecurityasserts"];
        set => this.SetValue ("includesecurityasserts", value);
    }

    /// <summary>
    /// Gets or sets a boolean value that indicates if column binary flags set by the server are ignored.
    /// </summary>
    /// <remarks>Default value is <c>true</c>.</remarks>
    [Category ("Advanced")]
    [DisplayName ("Respect Binary Flags")]
    [Description ("Should binary flags on column metadata be respected.")]
    [RefreshProperties (RefreshProperties.All)]
    [DefaultValue (true)]
    public bool RespectBinaryFlags
    {
        get => (bool) this.values ["respectbinaryflags"];
        set => this.SetValue ("respectbinaryflags", value);
    }

    /// <summary>
    /// Gets or sets a boolean value that indicates if <b>TINYINT(1)</b> shound be treated as a <b>BOOLEAN</b>.
    /// </summary>
    /// <remarks>Default value is <c>true</c>.</remarks>
    [Category ("Advanced")]
    [DisplayName ("Treat Tiny As Boolean")]
    [Description ("Should the provider treat TINYINT(1) columns as boolean.")]
    [RefreshProperties (RefreshProperties.All)]
    [DefaultValue (true)]
    public bool TreatTinyAsBoolean
    {
        get => (bool) this.values ["treattinyasboolean"];
        set => this.SetValue ("treattinyasboolean", value);
    }

    /// <summary>
    /// Gets or sets a boolean value that indicates if the provider expects user variables in the SQL.
    /// </summary>
    /// <remarks>Default value is <c>false</c>.</remarks>
    [Category ("Advanced")]
    [DisplayName ("Allow User Variables")]
    [Description ("Should the provider expect user variables to appear in the SQL.")]
    [RefreshProperties (RefreshProperties.All)]
    [DefaultValue (false)]
    public bool AllowUserVariables
    {
        get => (bool) this.values ["allowuservariables"];
        set => this.SetValue ("allowuservariables", value);
    }

    /// <summary>
    /// Gets or sets a boolean value that indicates if the session should be interactive.
    /// </summary>
    /// <remarks>Default value is <c>false</c>.</remarks>
    [Category ("Advanced")]
    [DisplayName ("Interactive Session")]
    [Description ("Should this session be considered interactive?")]
    [RefreshProperties (RefreshProperties.All)]
    [DefaultValue (false)]
    public bool InteractiveSession
    {
        get => (bool) this.values ["interactivesession"];
        set => this.SetValue ("interactivesession", value);
    }

    /// <summary>
    /// Gets or sets a boolean value that indicates if server functions should be treated as returning a string.
    /// </summary>
    /// <remarks>Default value is <c>false</c>.</remarks>
    [Category ("Advanced")]
    [DisplayName ("Functions Return String")]
    [Description ("Should all server functions be treated as returning string?")]
    [DefaultValue (false)]
    public bool FunctionsReturnString
    {
        get => (bool) this.values ["functionsreturnstring"];
        set => this.SetValue ("functionsreturnstring", value);
    }

    /// <summary>
    /// Gets or sets a boolean value that indicates if the server should report affected rows instead of found rows.
    /// </summary>
    /// <remarks>Default value is <c>false</c>.</remarks>
    [Category ("Advanced")]
    [DisplayName ("Use Affected Rows")]
    [Description ("Should the returned affected row count reflect affected rows instead of found rows?")]
    [DefaultValue (false)]
    public bool UseAffectedRows
    {
        get => (bool) this.values ["useaffectedrows"];
        set => this.SetValue ("useaffectedrows", value);
    }

    /// <summary>
    /// Gets or sets a boolean value that indicates if items of data type <b>BINARY(16)</b> should be treated as guids.
    /// </summary>
    /// <remarks>Default value is <c>false</c>.</remarks>
    [Category ("Advanced")]
    [DisplayName ("Old Guids")]
    [Description ("Treat binary(16) columns as guids")]
    [DefaultValue (false)]
    public bool OldGuids
    {
        get => (bool) this.values ["oldguids"];
        set => this.SetValue ("oldguids", value);
    }

    /// <summary>
    /// Gets or sets a boolean value that indicates if SQL Server syntax should be allowed by supporting square brackets
    /// around symbols instead of backticks.
    /// </summary>
    /// <remarks>Default value is <c>false</c>.</remarks>
    [Category ("Advanced")]
    [DisplayName ("Sql Server Mode")]
    [Description (
        "Allow Sql Server syntax.  " +
        "A value of yes allows symbols to be enclosed with [] instead of ``.  This does incur " +
        "a performance hit so only use when necessary."
    )]
    [DefaultValue (false)]
    public bool SqlServerMode
    {
        get => (bool) this.values ["sqlservermode"];
        set => this.SetValue ("sqlservermode", value);
    }

    /// <summary>
    /// Gets or sets a boolean value that indicates if caching of TableDirect commands is enabled.
    /// </summary>
    /// <remarks>Default value is <c>false</c>.</remarks>
    [Category ("Advanced")]
    [DisplayName ("Table Cache")]
    [Description (
        @"Enables or disables caching of TableDirect command.  
            A value of yes enables the cache while no disables it."
    )]
    [DefaultValue (false)]
    public bool TableCaching
    {
        get => (bool) this.values ["tablecaching"];
        set => this.SetValue ("tablecaching", value);
    }

    /// <summary>
    /// Gets or sets the seconds for how long a TableDirect result should be cached.
    /// </summary>
    /// <remarks>Default value is 0.</remarks>
    [Category ("Advanced")]
    [DisplayName ("Default Table Cache Age")]
    [Description (@"Specifies how long a TableDirect result should be cached in seconds.")]
    [DefaultValue (60)]
    public int DefaultTableCacheAge
    {
        get => (int) this.values ["defaulttablecacheage"];
        set => this.SetValue ("defaulttablecacheage", value);
    }

    /// <summary>
    /// Gets or sets a boolean value that indicates if stored routine parameters should be checked against the server.
    /// </summary>
    /// <remarks>Default value is <c>true</c>.</remarks>
    [Category ("Advanced")]
    [DisplayName ("Check Parameters")]
    [Description ("Indicates if stored routine parameters should be checked against the server.")]
    [DefaultValue (true)]
    public bool CheckParameters
    {
        get => (bool) this.values ["checkparameters"];
        set => this.SetValue ("checkparameters", value);
    }

    /// <summary>
    /// Gets or sets a boolean value that indicates if this connection will use replication.
    /// </summary>
    /// <remarks>Default value is <c>false</c>.</remarks>
    [Category ("Advanced")]
    [DisplayName ("Replication")]
    [Description ("Indicates if this connection is to use replicated servers.")]
    [DefaultValue (false)]
    public bool Replication
    {
        get => (bool) this.values ["replication"];
        set => this.SetValue ("replication", value);
    }

    /// <summary>
    /// Gets or sets the list of interceptors that can triage thrown MySqlExceptions.
    /// </summary>
    [Category ("Advanced")]
    [DisplayName ("Exception Interceptors")]
    [Description ("The list of interceptors that can triage thrown MySqlExceptions.")]
    public string ExceptionInterceptors
    {
        get => (string) this.values ["exceptioninterceptors"];
        set => this.SetValue ("exceptioninterceptors", value);
    }

    /// <summary>
    /// Gets or sets the list of interceptors that can intercept command operations.
    /// </summary>
    [Category ("Advanced")]
    [DisplayName ("Command Interceptors")]
    [Description ("The list of interceptors that can intercept command operations.")]
    public string CommandInterceptors
    {
        get => (string) this.values ["commandinterceptors"];
        set => this.SetValue ("commandinterceptors", value);
    }

    /// <summary>
    /// Gets or sets the event for the callback.
    /// </summary>
    internal FidoActionCallback FidoActionRequested { get; set; }

#endregion

#region Pooling Properties

    /// <summary>
    /// Gets or sets the lifetime of a pooled connection.
    /// </summary>
    /// <remarks>Default value is 0.</remarks>
    [Category ("Pooling")]
    [DisplayName ("Connection Lifetime")]
    [Description (
        "The minimum amount of time (in seconds) for this connection to " +
        "live in the pool before being destroyed."
    )]
    [RefreshProperties (RefreshProperties.All)]
    [DefaultValue (0)]
    public uint ConnectionLifeTime
    {
        get => (uint) this.values ["connectionlifetime"];
        set => this.SetValue ("connectionlifetime", value);
    }

    /// <summary>
    /// Gets or sets a boolean value indicating if connection pooling is enabled.
    /// </summary>
    /// <remarks>Default value is <c>true</c>.</remarks>
    [Category ("Pooling")]
    [Description (
        "When true, the connection object is drawn from the appropriate " +
        "pool, or if necessary, is created and added to the appropriate pool."
    )]
    [RefreshProperties (RefreshProperties.All)]
    [DefaultValue (true)]
    public bool Pooling
    {
        get => (bool) this.values ["pooling"];
        set => this.SetValue ("pooling", value);
    }

    /// <summary>
    /// Gets the minimum connection pool size.
    /// </summary>
    /// <remarks>Default value is 0.</remarks>
    [Category ("Pooling")]
    [DisplayName ("Minimum Pool Size")]
    [Description ("The minimum number of connections allowed in the pool.")]
    [RefreshProperties (RefreshProperties.All)]
    [DefaultValue (0)]
    public uint MinimumPoolSize
    {
        get => (uint) this.values ["minpoolsize"];
        set => this.SetValue ("minpoolsize", value);
    }

    /// <summary>
    /// Gets or sets the maximum connection pool setting.
    /// </summary>
    /// <remarks>Default value is 100.</remarks>
    [Category ("Pooling")]
    [DisplayName ("Maximum Pool Size")]
    [Description ("The maximum number of connections allowed in the pool.")]
    [RefreshProperties (RefreshProperties.All)]
    [DefaultValue (100)]
    public uint MaximumPoolSize
    {
        get => (uint) this.values ["maxpoolsize"];
        set => this.SetValue ("maxpoolsize", value);
    }

    /// <summary>
    /// Gets or sets a boolean value that indicates if the connection should be reset when retrieved
    /// from the pool.
    /// </summary>
    /// <remarks>Default value is <c>false</c>.</remarks>
    [Category ("Pooling")]
    [DisplayName ("Connection Reset")]
    [Description ("When true, indicates the connection state is reset when removed from the pool.")]
    [RefreshProperties (RefreshProperties.All)]
    [DefaultValue (false)]
    public bool ConnectionReset
    {
        get => (bool) this.values ["connectionreset"];
        set => this.SetValue ("connectionreset", value);
    }

    /// <summary>
    /// Gets or sets a boolean value that indicates whether the server variable settings are updated by a
    /// SHOW VARIABLES command each time a pooled connection is returned.
    /// </summary>
    /// <remarks>Default value is <c>false</c>.</remarks>
    [Category ("Pooling")]
    [DisplayName ("Cache Server Properties")]
    [Description ("When true, server properties will be cached after the first server in the pool is created")]
    [RefreshProperties (RefreshProperties.All)]
    [DefaultValue (false)]
    public bool CacheServerProperties
    {
        get => (bool) this.values ["cacheserverproperties"];
        set => this.SetValue ("cacheserverproperties", value);
    }

#endregion

#region Language and Character Set Properties

    /// <summary>
    /// Indicates whether the driver should treat binary BLOBs as UTF8.
    /// </summary>
    /// <remarks>Default value is <c>false</c>.</remarks>
    [DisplayName ("Treat Blobs As UTF8")]
    [Category ("Advanced")]
    [Description ("Should binary blobs be treated as UTF8")]
    [RefreshProperties (RefreshProperties.All)]
    [DefaultValue (false)]
    public bool TreatBlobsAsUTF8
    {
        get => (bool) this.values ["treatblobsasutf8"];
        set => this.SetValue ("treatblobsasutf8", value);
    }

    /// <summary>
    /// Gets or sets the pattern to match for the columns that should be treated as UTF8.
    /// </summary>
    [Category ("Advanced")]
    [Description ("Pattern that matches columns that should be treated as UTF8")]
    [RefreshProperties (RefreshProperties.All)]
    public string BlobAsUTF8IncludePattern
    {
        get => (string) this.values ["blobasutf8includepattern"];
        set => this.SetValue ("blobasutf8includepattern", value);
    }

    /// <summary>
    /// Gets or sets the pattern to match for the columns that should not be treated as UTF8.
    /// </summary>
    [Category ("Advanced")]
    [Description ("Pattern that matches columns that should not be treated as UTF8")]
    [RefreshProperties (RefreshProperties.All)]
    public string BlobAsUTF8ExcludePattern
    {
        get => (string) this.values ["blobasutf8excludepattern"];
        set => this.SetValue ("blobasutf8excludepattern", value);
    }

#endregion

#region Backwards compatibility properties

    [DisplayName ("Use Default Command Timeout For EF")]
    [Category ("Backwards Compatibility")]
    [Description ("Enforces the command timeout of EFMySqlCommand to the value provided in 'DefaultCommandTimeout' property")]
    [DefaultValue (false)]
    public bool UseDefaultCommandTimeoutForEF
    {
        get => (bool) this.values ["usedefaultcommandtimeoutforef"];
        set => this.SetValue ("usedefaultcommandtimeoutforef", value);
    }

#endregion

    /// <summary>
    /// Gets or sets a connection option.
    /// </summary>
    /// <param name="keyword">The keyword that identifies the connection option to modify.</param>
    public override object this [string keyword]
    {
        get
        {
            MySqlConnectionStringOption opt = this.GetOption (keyword);

            if (opt.ClassicGetter != null)
                return opt.ClassicGetter (this, opt);
            else if (opt.Getter != null)
                return opt.Getter (this, opt);
            else
                throw new ArgumentException (Resources.KeywordNotSupported, keyword);
        }
        set
        {
            MySqlConnectionStringOption opt = this.GetOption (keyword);

            if (opt.ClassicSetter != null)
                opt.ClassicSetter (this, opt, value);
            else if (opt.Setter != null)
                opt.Setter (this, opt, value);
            else
                throw new ArgumentException (Resources.KeywordNotSupported, keyword);
        }
    }

    public override void Clear ()
    {
        base.Clear ();

        lock (this)
        {
            foreach (MySqlConnectionStringOption option in Options.Options)
                if (option.DefaultValue != null)
                    this.values [option.Keyword] = option.DefaultValue;
                else
                    this.values [option.Keyword] = null;
        }
    }

    public override bool ContainsKey (string keyword)
    {
        MySqlConnectionStringOption option = Options.Get (keyword);
        return option != null;
    }

    public override bool Equals (object obj)
    {
        MySqlConnectionStringBuilder other = obj as MySqlConnectionStringBuilder;

        if (obj == null)
            return false;

        if (this.values.Count != other.values.Count)
            return false;

        foreach (KeyValuePair <string, object> kvp in this.values)
            if (other.values.ContainsKey (kvp.Key))
            {
                object v = other.values [kvp.Key];

                if (v == null && kvp.Value != null)
                    return false;

                if (kvp.Value == null && v != null)
                    return false;

                if (kvp.Value == null && v == null)
                    return true;

                if (!v.Equals (kvp.Value))
                    return false;
            }
            else
            {
                return false;
            }

        return true;
    }

    internal Regex GetBlobAsUTF8IncludeRegex ()
    {
        if (string.IsNullOrEmpty (this.BlobAsUTF8IncludePattern))
            return null;

        return new Regex (this.BlobAsUTF8IncludePattern);
    }

    internal Regex GetBlobAsUTF8ExcludeRegex ()
    {
        if (string.IsNullOrEmpty (this.BlobAsUTF8ExcludePattern))
            return null;

        return new Regex (this.BlobAsUTF8ExcludePattern);
    }

    internal override MySqlConnectionStringOption GetOption (string key)
    {
        MySqlConnectionStringOption option = Options.Get (key);

        if (option == null)
            throw new ArgumentException (Resources.KeywordNotSupported, key);
        else
            return option;
    }

    public override bool Remove (string keyword)
    {
        bool removed = false;

        lock (this)
        {
            removed = base.Remove (keyword);
        }

        if (!removed)
            return false;

        MySqlConnectionStringOption option = this.GetOption (keyword);

        lock (this)
        {
            this.values [option.Keyword] = option.DefaultValue;
        }

        return true;
    }

    internal override void SetInternalValue (string keyword, object value)
    {
        MySqlConnectionStringOption option = this.GetOption (keyword);
        option.ValidateValue (ref value);

        // remove all related keywords
        option.Clean (this);

        if (value != null)
            lock (this)
            {
                // set value for the given keyword
                this.values [option.Keyword] = value;
                base [keyword]               = value;
            }
    }

    /// <summary>
    /// Retrieves the value corresponding to the supplied key from this <see cref="MySqlConnectionStringBuilder"/>.
    /// </summary>
    /// <param name="keyword">The key of the item to retrieve.</param>
    /// <param name="value">The value corresponding to the <paramref name="keyword"/>.</param>
    /// <returns><see langword="true"/> if <paramref name="keyword"/> was found within the connection string; 
    /// otherwise, <see langword="false"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="keyword"/> contains a null value.</exception>
    public override bool TryGetValue (string keyword, out object value)
    {
        if (keyword == null)
            throw new ArgumentNullException (keyword);

        MySqlConnectionStringOption option = Options.Get (keyword);

        value = option == null ? null : this [keyword];
        return option != null;
    }
}
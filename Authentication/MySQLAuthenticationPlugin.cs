// Copyright (c) 2012, 2022, Oracle and/or its affiliates.
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
using System.Diagnostics;
using System.Text;
using EVESharp.Database.MySql;

namespace EVESharp.Database.MySql.Authentication;

/// <summary>
/// Defines the default behavior for an authentication plugin.
/// </summary>
public abstract class MySqlAuthenticationPlugin
{
    internal NativeDriver _driver;

    /// <summary>
    /// Handles the iteration of the multifactor authentication
    /// </summary>
    private int _mfaIteration = 1;

    /// <summary>
    /// Gets or sets the authentication data returned by the server.
    /// </summary>
    protected byte [] AuthenticationData;

    /// <summary>
    /// This is a factory method that is used only internally.  It creates an auth plugin based on the method type
    /// </summary>
    /// <param name="method"></param>
    /// <param name="driver"></param>
    /// <param name="authData"></param>
    /// <returns></returns>
    internal static MySqlAuthenticationPlugin GetPlugin (string method, NativeDriver driver, byte [] authData, int mfaIteration = 1)
    {
        if (method == "mysql_old_password")
        {
            driver.Close (true);
            throw new MySqlException (Resources.OldPasswordsNotSupported);
        }

        MySqlAuthenticationPlugin plugin = AuthenticationPluginManager.GetPlugin (method);

        if (plugin == null)
            throw new MySqlException (string.Format (Resources.UnknownAuthenticationMethod, method));

        plugin._driver       = driver;
        plugin._mfaIteration = mfaIteration;
        plugin.SetAuthData (authData);
        return plugin;
    }

    /// <summary>
    /// Gets the connection option settings.
    /// </summary>
    protected MySqlConnectionStringBuilder Settings => this._driver.Settings;

    /// <summary>
    /// Gets the server version associated with this authentication plugin.
    /// </summary>
    protected Version ServerVersion => new Version (this._driver.Version.Major, this._driver.Version.Minor, this._driver.Version.Build);

    internal ClientFlags Flags => this._driver.Flags;

    /// <summary>
    /// Gets the encoding assigned to the native driver.
    /// </summary>
    protected Encoding Encoding => this._driver.Encoding;

    /// <summary>
    /// Sets the authentication data required to encode, encrypt, or convert the password of the user.
    /// </summary>
    /// <param name="data">A byte array containing the authentication data provided by the server.</param>
    /// <remarks>This method may be overriden based on the requirements by the implementing authentication plugin.</remarks>
    protected virtual void SetAuthData (byte [] data)
    {
        this.AuthenticationData = data;
    }

    /// <summary>
    /// Defines the behavior when checking for constraints.
    /// </summary>
    /// <remarks>This method is intended to be overriden.</remarks>
    protected virtual void CheckConstraints () { }

    /// <summary>
    /// Throws a <see cref="MySqlException"/> that encapsulates the original exception.
    /// </summary>
    /// <param name="ex">The exception to encapsulate.</param>
    protected virtual void AuthenticationFailed (Exception ex)
    {
        string msg = string.Format (Resources.AuthenticationFailed, this.Settings.Server, this.GetUsername (), this.PluginName, ex.Message);
        throw new MySqlException (msg, ex);
    }

    /// <summary>
    /// Defines the behavior when authentication is successful.
    /// </summary>
    /// <remarks>This method is intended to be overriden.</remarks>
    protected virtual void AuthenticationSuccessful () { }

    /// <summary>
    /// Defines the behavior when more data is required from the server.
    /// </summary>
    /// <param name="data">The data returned by the server.</param>
    /// <returns>The data to return to the server.</returns>
    /// <remarks>This method is intended to be overriden.</remarks>
    protected virtual byte [] MoreData (byte [] data)
    {
        return null;
    }

    internal void Authenticate (bool reset)
    {
        this.CheckConstraints ();

        MySqlPacket packet = this._driver.Packet;

        // send auth response
        packet.WriteString (this.GetUsername ());

        // now write the password
        this.WritePassword (packet);

        if ((this.Flags & ClientFlags.CONNECT_WITH_DB) != 0 || reset)
            if (!string.IsNullOrEmpty (this.Settings.Database))
                packet.WriteString (this.Settings.Database);

        if (reset)
            packet.WriteInteger (8, 2);

        if ((this.Flags & ClientFlags.PLUGIN_AUTH) != 0)
            packet.WriteString (this.PluginName);

        this._driver.SetConnectAttrs ();
        this._driver.SendPacket (packet);

        // Read server response.
        packet = this.ReadPacket ();
        byte [] b = packet.Buffer;

        if (this.PluginName == "caching_sha2_password" && b [0] == 0x01)
            // React to the authentication type set by server: FAST, FULL.
            this.ContinueAuthentication (new byte [] {b [1]});

        // Auth switch request Protocol::AuthSwitchRequest.
        if (b [0] == 0xfe)
        {
            if (packet.IsLastPacket)
            {
                this._driver.Close (true);
                throw new MySqlException (Resources.OldPasswordsNotSupported);
            }
            else
            {
                this.HandleAuthChange (packet);
            }
        }

        // Auth request Protocol::AuthNextFactor.
        while (packet.Buffer [0] == 0x02)
        {
            ++this._mfaIteration;
            this.HandleMFA (packet);
        }

        this._driver.ReadOk (false);

        this.AuthenticationSuccessful ();
    }

    private void WritePassword (MySqlPacket packet)
    {
        bool   secure   = (this.Flags & ClientFlags.SECURE_CONNECTION) != 0;
        object password = this.GetPassword ();

        if (password is string)
        {
            if (secure)
                packet.WriteLenString ((string) password);
            else
                packet.WriteString ((string) password);
        }
        else if (password == null)
        {
            packet.WriteByte (0);
        }
        else if (password is byte [])
        {
            packet.Write (password as byte []);
        }
        else
        {
            throw new MySqlException ("Unexpected password format: " + password.GetType ());
        }
    }

    internal MySqlPacket ReadPacket ()
    {
        try
        {
            MySqlPacket p = this._driver.ReadPacket ();
            return p;
        }
        catch (MySqlException ex)
        {
            // Make sure this is an auth failed ex
            this.AuthenticationFailed (ex);
            return null;
        }
    }

    private void HandleMFA (MySqlPacket packet)
    {
        byte b = packet.ReadByte ();
        Debug.Assert (b == 0x02);

        this.NextPlugin (packet).ContinueAuthentication ();
    }

    private void HandleAuthChange (MySqlPacket packet)
    {
        byte b = packet.ReadByte ();
        Debug.Assert (b == 0xfe);

        this.NextPlugin (packet).ContinueAuthentication ();
    }

    private MySqlAuthenticationPlugin NextPlugin (MySqlPacket packet)
    {
        string  method   = packet.ReadString ();
        byte [] authData = new byte[packet.Length - packet.Position];
        Array.Copy (packet.Buffer, packet.Position, authData, 0, authData.Length);

        MySqlAuthenticationPlugin plugin = GetPlugin (method, this._driver, authData, this._mfaIteration);
        return plugin;
    }

    private void ContinueAuthentication (byte [] data = null)
    {
        MySqlPacket packet = this._driver.Packet;
        packet.Clear ();

        byte [] moreData = this.MoreData (data);

        while (moreData != null)
        {
            packet.Clear ();
            packet.Write (moreData);
            this._driver.SendPacket (packet);

            packet = this.ReadPacket ();
            byte prefixByte = packet.Buffer [0];

            if (prefixByte != 1)
                return;

            // A prefix of 0x01 means need more auth data.
            byte [] responseData = new byte[packet.Length - 1];
            Array.Copy (packet.Buffer, 1, responseData, 0, responseData.Length);
            moreData = this.MoreData (responseData);
        }

        // We get here if MoreData returned null but the last packet read was a more data packet.
        this.ReadPacket ();
    }

    /// <summary>
    /// Gets the password for the iteration of the multifactor authentication 
    /// </summary>
    /// <returns>A password</returns>
    protected string GetMFAPassword ()
    {
        switch (this._mfaIteration)
        {
            case 1:
            default:
                return this.Settings.Password;
            case 2: return this.Settings.Password2;
            case 3: return this.Settings.Password3;
        }
    }

    /// <summary>
    /// Gets the plugin name based on the authentication plugin type defined during the creation of this object.
    /// </summary>
    public abstract string PluginName { get; }

    /// <summary>
    /// Gets the user name associated to the connection settings.
    /// </summary>
    /// <returns>The user name associated to the connection settings.</returns>
    public virtual string GetUsername ()
    {
        return !string.IsNullOrWhiteSpace (this.Settings.UserID) ? this.Settings.UserID : Environment.UserName;
    }

    /// <summary>
    /// Gets the encoded, encrypted, or converted password based on the authentication plugin type defined during the creation of this object.
    /// This method is intended to be overriden.
    /// </summary>
    /// <returns>An object containing the encoded, encrypted, or converted password.</returns>
    public virtual object GetPassword ()
    {
        return null;
    }
}
// Copyright (c) 2021, Oracle and/or its affiliates.
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

using EVESharp.Database.MySql.Authentication.GSSAPI;
using EVESharp.Database.MySql.Authentication.SSPI;
using EVESharp.Database.MySql.Common;
using System;
using System.Text;
using EVESharp.Database.MySql;

namespace EVESharp.Database.MySql.Authentication;

/// <summary>
/// Enables connections to a user account set with the authentication_kerberos authentication plugin.
/// </summary>
internal class KerberosAuthenticationPlugin : MySqlAuthenticationPlugin
{
    public override string PluginName => "authentication_kerberos_client";
    protected       string Username   { get; private set; }
    protected       string Password   { get; private set; }
    private         string _servicePrincipal;
    private         string _realm;

    private const string PACKAGE = "Kerberos";

    private GssapiMechanism     _gssapiMechanism;
    private SspiSecurityContext _sspiSecurityContext;

    protected override void SetAuthData (byte [] data)
    {
        this.Username = this.GetUsername ();
        this.Password = this.GetMFAPassword ();

        //Protocol::AuthSwitchRequest plugin data contains:
        // int<2> SPN string length
        // string<VAR> SPN string
        // int<2> User Principal Name realm string length
        // string<VAR> User Principal Name realm string
        short servicePrincipalNameLength = BitConverter.ToInt16 (data, 0);

        if (servicePrincipalNameLength > data.Length)
            return; // not an AuthSwitchRequest

        this._servicePrincipal = this.Encoding.GetString (data, 2, servicePrincipalNameLength);
        short userPrincipalRealmLength = BitConverter.ToInt16 (data, servicePrincipalNameLength + 2);
        this._realm = this.Encoding.GetString (data, servicePrincipalNameLength + 4, userPrincipalRealmLength);
    }

    public override string GetUsername ()
    {
        this.Username = string.IsNullOrWhiteSpace (this.Username) ? this.Settings.UserID : this.Username;

        // If no password is provided, MySQL user and Windows logged-in user should match
        if (Platform.IsWindows () && !string.IsNullOrWhiteSpace (this.Username) && string.IsNullOrWhiteSpace (this.Password) &&
            this.Username != Environment.UserName)
            throw new MySqlException (string.Format (Resources.UnmatchedWinUserAndMySqlUser, this.Username, Environment.UserName));

        if (string.IsNullOrWhiteSpace (this.Username))
            try
            {
                // Try to obtain the user name from a cached TGT
                this.Username = new GssCredentials ().UserName.Trim ();
            }
            catch (Exception)
            {
                // Fall-back to system login user
                this.Username = base.GetUsername ();
            }

        int posAt = this.Username.IndexOf ('@');
        this.Settings.UserID = posAt < 0 ? this.Username : this.Username.Substring (0, posAt);
        return this.Settings.UserID;
    }

    protected override byte [] MoreData (byte [] data)
    {
        if (Platform.IsWindows ())
        {
            if (this._sspiSecurityContext == null)
            {
                this.Username = $"{this.Username}@{this._realm}";

                SspiCredentials sspiCreds = string.IsNullOrWhiteSpace (this.Password)
                    ? new SspiCredentials (PACKAGE)
                    : new SspiCredentials (this._servicePrincipal, this.Username, this.Password, this._realm, PACKAGE);

                this._sspiSecurityContext = new SspiSecurityContext (sspiCreds);
            }

            ContextStatus status = this._sspiSecurityContext.InitializeSecurityContext (out byte [] clientBlob, data, this._servicePrincipal);

            if (clientBlob.Length == 0 && status == ContextStatus.Accepted)
                return null;
            else
                return clientBlob;
        }
        else
        {
            if (this._gssapiMechanism == null)
            {
                this.Username         = $"{this.Username}@{this._realm}";
                this._gssapiMechanism = new GssapiMechanism (this.Username, this.Password, this._servicePrincipal);
            }

            byte [] response = this._gssapiMechanism.Challenge (data);

            if (response.Length == 0 && this._gssapiMechanism.gssContext.IsEstablished)
                return null;
            else
                return response;
        }
    }
}
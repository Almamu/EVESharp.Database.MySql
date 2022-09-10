// Copyright (c) 2022, Oracle and/or its affiliates.
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

using EVESharp.Database.MySql.Authentication.FIDO;
using System;
using System.IO;
using System.Text;

namespace EVESharp.Database.MySql.Authentication;

internal class FidoAuthenticationPlugin : MySqlAuthenticationPlugin
{
    public override string PluginName => "authentication_fido_client";

    // Constants
    private const int CLIENT_DATA_LENGTH          = 32;
    private const int RELYING_PARTY_ID_MAX_LENGTH = 255;

    // Fields
    private byte [] _clientDataHash;
    private byte [] _credentialId;
    private string  _relyingPartyId;

    protected override void SetAuthData (byte [] data)
    {
        if (data.Length > 0)
            this.ParseChallenge (data);
        else
            throw new MySqlException ("FIDO registration missing.");
    }

    protected override byte [] MoreData (byte [] data)
    {
        return this.SignChallenge ();
    }

    /// <summary>
    /// Method that parse the challenge received from server during authentication process.
    /// This method extracts salt, relying party name and set it in the <see cref="FidoAssertion"/> object.
    /// </summary>
    /// <param name="challenge">Buffer holding the server challenge.</param>
    /// <exception cref="MySqlException">Thrown if an error occurs while parsing the challenge.</exception>
    private void ParseChallenge (byte [] challenge)
    {
        // client_data_hash length should be 32 bytes
        int clientDataLength = challenge [0];

        if (clientDataLength != CLIENT_DATA_LENGTH)
            throw new MySqlException ("Challenge received is corrupt.");

        // client_data_hash
        this._clientDataHash = new byte[clientDataLength];
        Array.Copy (challenge, 1, this._clientDataHash, 0, clientDataLength);

        // relyting_party_id length cannot be more than 255
        int relyingPartyIdLength = challenge [clientDataLength + 1];

        if (relyingPartyIdLength > RELYING_PARTY_ID_MAX_LENGTH)
            throw new MySqlException ("Challenge received is corrupt.");

        // relying_party_id
        this._relyingPartyId = this.Encoding.GetString (challenge, clientDataLength + 2, relyingPartyIdLength);

        // credential_id length
        int credentialIdLength = challenge [clientDataLength + relyingPartyIdLength + 2];
        // credential_id
        this._credentialId = new byte[credentialIdLength];
        Array.Copy (challenge, clientDataLength + relyingPartyIdLength + 3, this._credentialId, 0, credentialIdLength);
    }

    /// <summary>
    /// Method to obtains an assertion from a FIDO device.
    /// </summary>
    private byte [] SignChallenge ()
    {
        string devicePath;

        using (FidoDeviceInfo fidoDeviceInfo = new FidoDeviceInfo ())
        {
            devicePath = fidoDeviceInfo.Path;
        }

        using (FidoAssertion fidoAssertion = new FidoAssertion ())
        {
            using (FidoDevice fidoDevice = new FidoDevice ())
            {
                fidoAssertion.ClientDataHash = this._clientDataHash;
                fidoAssertion.Rp             = this._relyingPartyId;
                fidoAssertion.AllowCredential (this._credentialId);

                fidoDevice.Open (devicePath);

                if (this._driver.Settings.FidoActionRequested != null)
                    this._driver.Settings.FidoActionRequested?.Invoke ();
                else
                    throw new MySqlException ("An event handler for FidoActionRequested was not specified.");

                fidoDevice.GetAssert (fidoAssertion);

                FidoAssertionStatement fidoAssertionStatement = fidoAssertion.GetFidoAssertionStatement ();

                int responseLength = fidoAssertionStatement.SignatureLen + fidoAssertionStatement.AuthDataLen +
                                     this.GetLengthSize ((ulong) fidoAssertionStatement.SignatureLen) +
                                     this.GetLengthSize ((ulong) fidoAssertionStatement.AuthDataLen);

                MySqlPacket response = new MySqlPacket (new MemoryStream (responseLength));
                response.WriteLength (fidoAssertionStatement.AuthDataLen);
                response.Write (fidoAssertionStatement.AuthData.ToArray ());
                response.WriteLength (fidoAssertionStatement.SignatureLen);
                response.Write (fidoAssertionStatement.Signature.ToArray ());

                return response.Buffer;
            }
        }
    }

    private int GetLengthSize (ulong length)
    {
        if (length < 251UL)
            return 1;

        if (length < 65536L)
            return 3;

        if (length < 16777216UL)
            return 4;

        return 9;
    }
}
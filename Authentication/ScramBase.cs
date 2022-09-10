﻿// Copyright (c) 2020, Oracle and/or its affiliates.
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
using System.Security.Cryptography;
using System.Text;
using EVESharp.Database.MySql;

namespace EVESharp.Database.MySql.Authentication;

/// <summary>
/// Base class to handle SCRAM authentication methods
/// </summary>
internal abstract class ScramBase : MySqlSASLPlugin
{
    /// <summary>
    /// Defines the state of the authentication process.
    /// </summary>
    internal enum AuthState
    {
        INITIAL,
        FINAL,
        VALIDATE
    }

    protected string Host     { get; private set; }
    protected string Username { get; private set; }
    protected string Password { get; private set; }

    internal string    _cnonce, client;
    internal byte []   salted,  auth;
    internal AuthState _state;

    protected ScramBase (string username, string password, string host)
    {
        this.Host     = host;
        this.Username = username;
        this.Password = password;
    }

    /// <summary>
    /// Gets the name of the method.
    /// </summary>
    internal abstract string MechanismName { get; }

    /// <summary>
    /// Parses the server's challenge token and returns the next challenge response.
    /// </summary>
    /// <returns>The next challenge response.</returns>
    internal byte [] Challenge (byte [] token)
    {
        byte [] response = null;

        switch (this._state)
        {
            case AuthState.INITIAL:
                response    = this.ClientInitial ();
                this._state = AuthState.FINAL;
                break;
            case AuthState.FINAL:
                response    = this.ProcessServerResponse (token);
                this._state = AuthState.VALIDATE;
                break;
            case AuthState.VALIDATE:
                this.ValidateAuth (token);
                break;
            default: throw new Exception ("Unexpected SCRAM authentication message.");
        }

        return response;
    }

    /// <summary>
    /// Builds up the client-first message.
    /// </summary>
    /// <returns>An array of bytes containig the client-first message.</returns>
    internal byte [] ClientInitial ()
    {
        this._cnonce = this._cnonce ?? GetRandomBytes (32);
        this.client  = $"n={Normalize (this.Username)},r={this._cnonce}";
        return Encoding.UTF8.GetBytes ($"n,a={Normalize (this.Username)}," + this.client);
    }

    /// <summary>
    /// Processes the server response from the client-first message and 
    /// builds up the client-final message.
    /// </summary>
    /// <param name="data">Response from the server.</param>
    /// <returns>An array of bytes containing the client-final message.</returns>
    internal byte [] ProcessServerResponse (byte [] data)
    {
        string                    response = Encoding.UTF8.GetString (data, 0, data.Length);
        Dictionary <char, string> tokens   = ParseServerChallenge (response);

        if (!tokens.TryGetValue ('s', out string salt))
            throw new MySqlException (string.Format (Resources.AuthenticationFailed, this.Host, this.Username, this.MechanismName, "salt is missing."));

        if (!tokens.TryGetValue ('r', out string snonce))
            throw new MySqlException (string.Format (Resources.AuthenticationFailed, this.Host, this.Username, this.MechanismName, "nonce is missing."));

        if (!tokens.TryGetValue ('i', out string iterations))
            throw new MySqlException (
                string.Format (Resources.AuthenticationFailed, this.Host, this.Username, this.MechanismName, "iteration count is missing.")
            );

        if (!tokens ['r'].StartsWith (this._cnonce, StringComparison.Ordinal))
            throw new MySqlException (string.Format (Resources.AuthenticationFailed, this.Host, this.Username, this.MechanismName, "invalid nonce."));

        if (!int.TryParse (iterations, out int count))
            throw new MySqlException (string.Format (Resources.AuthenticationFailed, this.Host, this.Username, this.MechanismName, "invalid iteration count."));

        byte [] password = Encoding.UTF8.GetBytes (this.Password);
        this.salted = this.Hi (password, Convert.FromBase64String (salt), count);
        Array.Clear (password, 0, password.Length);

        string withoutProof = "c=" + Convert.ToBase64String (Encoding.ASCII.GetBytes ($"n,a={this.Username},")) + ",r=" + snonce;
        this.auth = Encoding.UTF8.GetBytes (this.client + "," + response + "," + withoutProof);

        byte [] ckey = this.HMAC (this.salted,  Encoding.ASCII.GetBytes ("Client Key"));
        Xor (ckey, this.HMAC (this.Hash (ckey), this.auth));

        return Encoding.UTF8.GetBytes (withoutProof + ",p=" + Convert.ToBase64String (ckey));
    }

    /// <summary>
    /// Validates the server response.
    /// </summary>
    /// <param name="data">Server-final message</param>
    internal void ValidateAuth (byte [] data)
    {
        string response = Encoding.UTF8.GetString (data, 0, data.Length);

        if (!response.StartsWith ("v=", StringComparison.Ordinal))
            throw new MySqlException (
                string.Format (Resources.AuthenticationFailed, this.Host, this.Username, this.MechanismName, "challenge did not start with a signature.")
            );

        byte [] signature  = Convert.FromBase64String (response.Substring (2));
        byte [] skey       = this.HMAC (this.salted, Encoding.ASCII.GetBytes ("Server Key"));
        byte [] calculated = this.HMAC (skey,        this.auth);

        if (signature.Length != calculated.Length)
            throw new MySqlException (
                string.Format (
                    Resources.AuthenticationFailed, this.Host, this.Username, this.MechanismName, "challenge contained a signature with an invalid length."
                )
            );

        for (int i = 0; i < signature.Length; i++)
            if (signature [i] != calculated [i])
                throw new MySqlException (
                    string.Format (Resources.AuthenticationFailed, this.Host, this.Username, this.MechanismName, "challenge contained an invalid signature.")
                );
    }

    private static string Normalize (string str)
    {
        StringBuilder builder  = new StringBuilder ();
        string        prepared = SaslPrep (str);

        for (int i = 0; i < prepared.Length; i++)
            switch (prepared [i])
            {
                case ',':
                    builder.Append ("=2C");
                    break;
                case '=':
                    builder.Append ("=3D");
                    break;
                default:
                    builder.Append (prepared [i]);
                    break;
            }

        return builder.ToString ();
    }

    /// <summary>
    /// Creates the HMAC SHA1 context.
    /// </summary>
    /// <returns>The HMAC context.</returns>
    /// <param name="key">The secret key.</param>
    protected abstract KeyedHashAlgorithm CreateHMAC (byte [] key);

    /// <summary>
    /// Apply the HMAC keyed algorithm.
    /// </summary>
    /// <returns>The results of the HMAC keyed algorithm.</returns>
    /// <param name="key">The key.</param>
    /// <param name="str">The string.</param>
    private byte [] HMAC (byte [] key, byte [] str)
    {
        using (KeyedHashAlgorithm hmac = this.CreateHMAC (key))
        {
            return hmac.ComputeHash (str);
        }
    }

    /// <summary>
    /// Applies the cryptographic hash function.
    /// </summary>
    /// <returns>The results of the hash.</returns>
    /// <param name="str">The string.</param>
    protected abstract byte [] Hash (byte [] str);

    /// <summary>
    /// Applies the exclusive-or operation to combine two octet strings.
    /// </summary>
    /// <param name="a">The alpha component.</param>
    /// <param name="b">The blue component.</param>
    private static void Xor (byte [] a, byte [] b)
    {
        for (int i = 0; i < a.Length; i++)
            a [i] = (byte) (a [i] ^ b [i]);
    }

    // Hi(str, salt, i):
    //
    // U1   := HMACSHA1(str, salt + INT(1))
    // U2   := HMACSHA1(str, U1)
    // ...
    // Ui-1 := HMACSHA1(str, Ui-2)
    // Ui   := HMACSHA1(str, Ui-1)
    //
    // Hi := U1 XOR U2 XOR ... XOR Ui
    //
    // where "i" is the iteration count, "+" is the string concatenation
    // operator, and INT(g) is a 4-octet encoding of the integer g, most
    // significant octet first.
    //
    // Hi() is, essentially, PBKDF2 [RFC2898] with HMACSHA1() as the
    // pseudorandom function (PRF) and with dkLen == output length of
    // HMACSHA1() == output length of H().
    private byte [] Hi (byte [] str, byte [] salt, int count)
    {
        using (KeyedHashAlgorithm hmac = this.CreateHMAC (str))
        {
            byte [] salt1 = new byte[salt.Length + 4];
            byte [] hi, u1;

            Buffer.BlockCopy (salt, 0, salt1, 0, salt.Length);
            salt1 [salt1.Length - 1] = (byte) 1;

            hi = u1 = hmac.ComputeHash (salt1);

            for (int i = 1; i < count; i++)
            {
                byte [] u2 = hmac.ComputeHash (u1);
                Xor (hi, u2);
                u1 = u2;
            }

            return hi;
        }
    }

    private static Dictionary <char, string> ParseServerChallenge (string challenge)
    {
        Dictionary <char, string> results = new Dictionary <char, string> ();

        foreach (string part in challenge.Split (','))
            if (part [1] == '=')
                results.Add (part [0], part.Substring (2));

        return results;
    }
}
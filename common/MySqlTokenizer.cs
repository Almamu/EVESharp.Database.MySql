// Copyright (c) 2004, 2021, Oracle and/or its affiliates.
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
using System.Diagnostics;
using System.Text;

namespace EVESharp.Database.MySql.Common;

internal class MySqlTokenizer
{
    private string _sql;

    public MySqlTokenizer ()
    {
        this.BackslashEscapes = true;
        this.MultiLine        = true;
        this.Position         = 0;
    }

    public MySqlTokenizer (string input)
        : this ()
    {
        this._sql = input;
    }

#region Properties

    public string Text
    {
        get => this._sql;
        set
        {
            this._sql     = value;
            this.Position = 0;
        }
    }

    public bool AnsiQuotes { get; set; }

    public bool BackslashEscapes { get; set; }

    public bool MultiLine { get; set; }

    public bool SqlServerMode { get; set; }

    public bool Quoted { get; private set; }

    public bool IsComment { get; private set; }

    public int StartIndex { get; set; }

    public int StopIndex { get; set; }

    public int Position { get; set; }

    public bool ReturnComments { get; set; }

#endregion

    public List <string> GetAllTokens ()
    {
        List <string> tokens = new List <string> ();
        string        token  = this.NextToken ();

        while (token != null)
        {
            tokens.Add (token);
            token = this.NextToken ();
        }

        return tokens;
    }

    public string NextToken ()
    {
        while (this.FindToken ())
        {
            string token = this._sql.Substring (this.StartIndex, this.StopIndex - this.StartIndex);
            return token;
        }

        return null;
    }

    public static bool IsParameter (string s)
    {
        if (string.IsNullOrEmpty (s))
            return false;

        if (s [0] == '?')
            return true;

        return s.Length > 1 && s [0] == '@' && s [1] != '@';
    }

    public string NextParameter ()
    {
        while (this.FindToken ())
        {
            if (this.StopIndex - this.StartIndex < 2)
            {
                if (IsParameter (this._sql.Substring (this.StartIndex, 1)))
                    return "?";
                else
                    continue;
            }

            char c1 = this._sql [this.StartIndex];
            char c2 = this._sql [this.StartIndex + 1];

            if (c1 == '?' ||
                (c1 == '@' && c2 != '@'))
                return this._sql.Substring (this.StartIndex, this.StopIndex - this.StartIndex);
        }

        return null;
    }

    public bool FindToken ()
    {
        this.IsComment  = this.Quoted    = false; // reset our flags
        this.StartIndex = this.StopIndex = -1;

        while (this.Position < this._sql.Length)
        {
            char c = this._sql [this.Position++];

            if (char.IsWhiteSpace (c))
                continue;

            if (c == '`' || c == '\'' || c == '"' || (c == '[' && this.SqlServerMode))
            {
                this.ReadQuotedToken (c);
            }
            else if (c == '#' || c == '-' || c == '/')
            {
                if (!this.ReadComment (c))
                    this.ReadSpecialToken ();
            }
            else
            {
                this.ReadUnquotedToken ();
            }

            if (this.StartIndex != -1)
                return true;
        }

        return false;
    }

    public string ReadParenthesis ()
    {
        StringBuilder sb    = new StringBuilder ("(");
        int           start = this.StartIndex;
        string        token = this.NextToken ();

        while (true)
        {
            if (token == null)
                throw new InvalidOperationException ("Unable to parse SQL");

            sb.Append (token);

            if (token == ")" && !this.Quoted)
                break;

            token = this.NextToken ();
        }

        return sb.ToString ();
    }

    private bool ReadComment (char c)
    {
        // make sure the comment starts correctly
        if (c == '/' && (this.Position >= this._sql.Length || this._sql [this.Position] != '*'))
            return false;

        if (c == '-' && (this.Position + 1 >= this._sql.Length || this._sql [this.Position] != '-' || this._sql [this.Position + 1] != ' '))
            return false;

        string endingPattern = "\n";

        if (this._sql [this.Position] == '*')
            endingPattern = "*/";

        int startingIndex = this.Position - 1;

        int index = this._sql.IndexOf (endingPattern, this.Position);

        if (endingPattern == "\n")
            index = this._sql.IndexOf ('\n', this.Position);

        if (index == -1)
            index = this._sql.Length - 1;
        else
            index += endingPattern.Length;

        this.Position = index;

        if (this.ReturnComments)
        {
            this.StartIndex = startingIndex;
            this.StopIndex  = index;
            this.IsComment  = true;
        }

        return true;
    }

    private void CalculatePosition (int start, int stop)
    {
        this.StartIndex = start;
        this.StopIndex  = stop;

        if (!this.MultiLine)
            return;
    }

    private void ReadUnquotedToken ()
    {
        this.StartIndex = this.Position - 1;

        if (!this.IsSpecialCharacter (this._sql [this.StartIndex]))
            while (this.Position < this._sql.Length)
            {
                char c = this._sql [this.Position];

                if (char.IsWhiteSpace (c))
                    break;

                if (this.IsSpecialCharacter (c))
                    break;

                this.Position++;
            }

        this.Quoted    = false;
        this.StopIndex = this.Position;
    }

    private void ReadSpecialToken ()
    {
        this.StartIndex = this.Position - 1;

        Debug.Assert (this.IsSpecialCharacter (this._sql [this.StartIndex]));

        this.StopIndex = this.Position;
        this.Quoted    = false;
    }

    /// <summary>
    ///  Read a single quoted identifier from the stream
    /// </summary>
    /// <param name="quoteChar"></param>
    /// <returns></returns>
    private void ReadQuotedToken (char quoteChar)
    {
        if (quoteChar == '[')
            quoteChar = ']';

        this.StartIndex = this.Position - 1;
        bool escaped = false;

        bool found = false;

        while (this.Position < this._sql.Length)
        {
            char c = this._sql [this.Position];

            if (c == quoteChar && !escaped)
            {
                found = true;
                break;
            }

            if (escaped)
                escaped = false;
            else if (c == '\\' && this.BackslashEscapes)
                escaped = true;

            this.Position++;
        }

        if (found)
            this.Position++;

        this.Quoted    = found;
        this.StopIndex = this.Position;
    }

    private bool IsQuoteChar (char c)
    {
        return c == '`' || c == '\'' || c == '\"';
    }

    internal bool IsParameterMarker (char c)
    {
        return c == '@' || c == '?';
    }

    private bool IsSpecialCharacter (char c)
    {
        if (char.IsLetterOrDigit (c) ||
            c == '$' || c == '_' || c == '.')
            return false;

        if (this.IsParameterMarker (c))
            return false;

        return true;
    }
}
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
using System.Data;
using System.Globalization;
using System.Linq;
using System.Text;
using EVESharp.Database.MySql;

namespace EVESharp.Database.MySql;

/// <summary>
/// Summary description for StoredProcedure.
/// </summary>
internal class StoredProcedure : PreparableStatement
{
    private string _outSelect;

    // Prefix used for to generate inout or output parameters names
    internal const string ParameterPrefix = "_cnet_param_";
    private        string resolvedCommandText;

    public StoredProcedure (MySqlCommand cmd, string text)
        : base (cmd, text) { }

    private MySqlParameter GetReturnParameter ()
    {
        return this.Parameters?.Cast <MySqlParameter> ().FirstOrDefault (p => p.Direction == ParameterDirection.ReturnValue);
    }

    public bool ServerProvidingOutputParameters { get; private set; }

    public override string ResolvedCommandText => this.resolvedCommandText;

    internal string GetCacheKey (string spName)
    {
        string        retValue = string.Empty;
        StringBuilder key      = new StringBuilder (spName);
        key.Append ("(");
        string delimiter = "";

        foreach (MySqlParameter p in this.command.Parameters)
            if (p.Direction == ParameterDirection.ReturnValue)
            {
                retValue = "?=";
            }
            else
            {
                key.AppendFormat (CultureInfo.InvariantCulture, "{0}?", delimiter);
                delimiter = ",";
            }

        key.Append (")");
        return retValue + key.ToString ();
    }

    private ProcedureCacheEntry GetParameters (string procName)
    {
        string              procCacheKey = this.GetCacheKey (procName);
        ProcedureCacheEntry entry        = this.Connection.ProcedureCache.GetProcedure (this.Connection, procName, procCacheKey);
        return entry;
    }

    public static string GetFlags (string dtd)
    {
        int x = dtd.Length - 1;

        while (x > 0 && (char.IsLetterOrDigit (dtd [x]) || dtd [x] == ' '))
            x--;

        string dtdSubstring = dtd.Substring (x);
        return StringUtility.ToUpperInvariant (dtdSubstring);
    }

    internal static string FixProcedureName (string spName)
    {
        if (IsSyntacticallyCorrect (spName))
            return spName;

        return $"`{spName.Replace ("`", "``")}`";
    }

    /// <summary>
    /// Verify if the string passed as argument is syntactically correct.
    /// </summary>
    /// <param name="spName">String to be analyzed</param>
    /// <returns>true if is correct; otherwise, false.</returns>
    internal static bool IsSyntacticallyCorrect (string spName)
    {
        const char backtick = '`', dot = '.';

        char [] spNameArray  = spName.ToArray ();
        bool    quoted       = spName.StartsWith ("`");
        bool    splittingDot = false;

        for (int i = 1; i < spNameArray.Length; i++)
            if (spNameArray [i] == backtick)
            {
                // We are in quoted mode.
                if (quoted)
                {
                    // we are not in the last char of the string.
                    if (i < spNameArray.Length - 1)
                    {
                        // Get the next char.
                        char nextChar = spNameArray [i + 1];

                        // If the next char are neither a dot nor a backtick,
                        // it means the input string is not well quoted and exits the loop.
                        if (nextChar != dot && nextChar != backtick)
                            return false;

                        // If the next char is a backtick, move forward 2 positions.
                        if (nextChar == backtick)
                            i++;

                        // If the next char is a dot, that means we are not in quoted mode anymore.
                        if (nextChar == dot)
                        {
                            quoted       = false;
                            splittingDot = true;
                            i++;
                        }
                    }
                }
                // Not quoted mode
                else
                {
                    // If the previous char is not a dot or the string does not end with a backtick,
                    // it means the input string is not well quoted and exits the loop;
                    // otherwise, enter quoted mode.
                    if (spNameArray [i - 1] != dot || !spName.EndsWith ("`"))
                        return false;

                    quoted = true;
                }
            }
            else if (spNameArray [i] == dot && !quoted)
            {
                if (splittingDot)
                    return false;
                else
                    splittingDot = true;
            }

        // If we reach to the very last char of the string, it means the string is well written.
        return true;
    }

    private MySqlParameter GetAndFixParameter (string spName, MySqlSchemaRow param, bool realAsFloat, MySqlParameter returnParameter)
    {
        string mode     = (string) param ["PARAMETER_MODE"];
        string pName    = (string) param ["PARAMETER_NAME"];
        string datatype = (string) param ["DATA_TYPE"];
        bool   unsigned = GetFlags (param ["DTD_IDENTIFIER"].ToString ()).IndexOf ("UNSIGNED") != -1;

        if (param ["ORDINAL_POSITION"].Equals (0))
        {
            if (returnParameter == null)
                throw new InvalidOperationException (string.Format (Resources.RoutineRequiresReturnParameter, spName));

            pName = returnParameter.ParameterName;
        }

        // make sure the parameters given to us have an appropriate type set if it's not already
        MySqlParameter p = this.command.Parameters.GetParameterFlexible (pName, true);

        if (!p.TypeHasBeenSet)
            p.MySqlDbType = MetaData.NameToType (datatype, unsigned, realAsFloat, this.Connection);

        return p;
    }

    private MySqlParameterCollection CheckParameters (string spName)
    {
        MySqlParameterCollection newParms        = new MySqlParameterCollection (this.command);
        MySqlParameter           returnParameter = this.GetReturnParameter ();

        ProcedureCacheEntry entry = this.GetParameters (spName);

        if (entry.procedure == null || entry.procedure.Rows.Count == 0)
            throw new InvalidOperationException (string.Format (Resources.RoutineNotFound, spName));

        bool realAsFloat = entry.procedure.Rows [0] ["SQL_MODE"].ToString ().IndexOf ("REAL_AS_FLOAT") != -1;

        foreach (MySqlSchemaRow param in entry.parameters.Rows)
            newParms.Add (this.GetAndFixParameter (spName, param, realAsFloat, returnParameter));

        return newParms;
    }

    public override void Resolve (bool preparing)
    {
        // check to see if we are already resolved
        if (this.ResolvedCommandText != null)
            return;

        this.ServerProvidingOutputParameters = this.Driver.SupportsOutputParameters && preparing;

        // first retrieve the procedure definition from our
        // procedure cache
        string spName = this.commandText;
        spName = FixProcedureName (spName);

        MySqlParameter returnParameter = this.GetReturnParameter ();

        MySqlParameterCollection parms = this.command.Connection.Settings.CheckParameters ? this.CheckParameters (spName) : this.Parameters;

        string setSql  = this.SetUserVariables (parms, preparing);
        string callSql = this.CreateCallStatement (spName, returnParameter, parms);
        string outSql  = this.CreateOutputSelect (parms, preparing);
        this.resolvedCommandText = string.Format ("{0}{1}{2}", setSql, callSql, outSql);
    }

    private string SetUserVariables (MySqlParameterCollection parms, bool preparing)
    {
        StringBuilder setSql = new StringBuilder ();

        if (this.ServerProvidingOutputParameters)
            return setSql.ToString ();

        string delimiter = string.Empty;

        foreach (MySqlParameter p in parms)
        {
            if (p.Direction != ParameterDirection.InputOutput)
                continue;

            string pName = "@" + p.BaseName;
            string uName = "@" + ParameterPrefix + p.BaseName;
            string sql   = string.Format ("SET {0}={1}", uName, pName);

            if (this.command.Connection.Settings.AllowBatch && !preparing)
            {
                setSql.AppendFormat (CultureInfo.InvariantCulture, "{0}{1}", delimiter, sql);
                delimiter = "; ";
            }
            else
            {
                MySqlCommand cmd = new MySqlCommand (sql, this.command.Connection);
                cmd.Parameters.Add (p);
                cmd.ExecuteNonQuery ();
            }
        }

        if (setSql.Length > 0)
            setSql.Append ("; ");

        return setSql.ToString ();
    }

    private string CreateCallStatement (string spName, MySqlParameter returnParameter, MySqlParameterCollection parms)
    {
        StringBuilder callSql = new StringBuilder ();

        string delimiter = string.Empty;

        foreach (MySqlParameter p in parms)
        {
            if (p.Direction == ParameterDirection.ReturnValue)
                continue;

            string pName = "@" + p.BaseName;
            string uName = "@" + ParameterPrefix + p.BaseName;

            bool useRealVar = p.Direction == ParameterDirection.Input || this.ServerProvidingOutputParameters;
            callSql.AppendFormat (CultureInfo.InvariantCulture, "{0}{1}", delimiter, useRealVar ? pName : uName);
            delimiter = ", ";
        }

        if (returnParameter == null)
            return string.Format ("CALL {0}({1})", spName, callSql.ToString ());
        else
            return string.Format ("SET @{0}{1}={2}({3})", ParameterPrefix, returnParameter.BaseName, spName, callSql.ToString ());
    }

    private string CreateOutputSelect (MySqlParameterCollection parms, bool preparing)
    {
        StringBuilder outSql = new StringBuilder ();

        string delimiter = string.Empty;

        foreach (MySqlParameter p in parms)
        {
            if (p.Direction == ParameterDirection.Input)
                continue;

            if ((p.Direction == ParameterDirection.InputOutput ||
                 p.Direction == ParameterDirection.Output) && this.ServerProvidingOutputParameters)
                continue;

            string pName = "@" + p.BaseName;
            string uName = "@" + ParameterPrefix + p.BaseName;

            outSql.AppendFormat (CultureInfo.InvariantCulture, "{0}{1}", delimiter, uName);
            delimiter = ", ";
        }

        if (outSql.Length == 0)
            return string.Empty;

        if (this.command.Connection.Settings.AllowBatch && !preparing)
            return string.Format (";SELECT {0}", outSql.ToString ());

        this._outSelect = string.Format ("SELECT {0}", outSql.ToString ());
        return string.Empty;
    }

    internal void ProcessOutputParameters (MySqlDataReader reader)
    {
        // We apparently need to always adjust our output types since the server
        // provided data types are not always right
        this.AdjustOutputTypes (reader);

        if ((reader.CommandBehavior & CommandBehavior.SchemaOnly) != 0)
            return;

        // now read the output parameters data row
        reader.Read ();

        string prefix = "@" + ParameterPrefix;

        for (int i = 0; i < reader.FieldCount; i++)
        {
            string fieldName = reader.GetName (i);

            if (fieldName.StartsWith (prefix, StringComparison.OrdinalIgnoreCase))
                fieldName = fieldName.Remove (0, prefix.Length);

            MySqlParameter parameter = this.command.Parameters.GetParameterFlexible (fieldName, true);
            parameter.Value = reader.GetValue (i);
        }
    }

    private void AdjustOutputTypes (MySqlDataReader reader)
    {
        // since MySQL likes to return user variables as strings
        // we reset the types of the readers internal value objects
        // this will allow those value objects to parse the string based
        // return values
        for (int i = 0; i < reader.FieldCount; i++)
        {
            string fieldName = reader.GetName (i);

            if (fieldName.IndexOf (ParameterPrefix) != -1)
                fieldName = fieldName.Remove (0, ParameterPrefix.Length + 1);

            MySqlParameter parameter = this.command.Parameters.GetParameterFlexible (fieldName, true);

            IMySqlValue v = MySqlField.GetIMySqlValue (parameter.MySqlDbType);

            if (v is MySqlBit)
            {
                MySqlBit bit = (MySqlBit) v;
                bit.ReadAsString = true;
                reader.ResultSet.SetValueObject (i, bit);
            }
            else
            {
                reader.ResultSet.SetValueObject (i, v);
            }
        }
    }

    public override void Close (MySqlDataReader reader)
    {
        base.Close (reader);

        if (string.IsNullOrEmpty (this._outSelect))
            return;

        if ((reader.CommandBehavior & CommandBehavior.SchemaOnly) != 0)
            return;

        MySqlCommand cmd = new MySqlCommand (this._outSelect, this.command.Connection);

        using (MySqlDataReader rdr = cmd.ExecuteReader (reader.CommandBehavior))
        {
            this.ProcessOutputParameters (rdr);
        }
    }
}
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
using System.Collections.Generic;
using System.Security;
using System.Text;
using System.Text.RegularExpressions;

namespace EVESharp.Database.MySql;

[Flags]
internal enum ColumnFlags : int
{
    NOT_NULL       = 1,
    PRIMARY_KEY    = 2,
    UNIQUE_KEY     = 4,
    MULTIPLE_KEY   = 8,
    BLOB           = 16,
    UNSIGNED       = 32,
    ZERO_FILL      = 64,
    BINARY         = 128,
    ENUM           = 256,
    AUTO_INCREMENT = 512,
    TIMESTAMP      = 1024,
    SET            = 2048,
    NUMBER         = 32768
};

/// <summary>
/// Summary description for Field.
/// </summary>
internal class MySqlField
{
#region Fields

    // public fields
    public string   CatalogName;
    public int      ColumnLength;
    public string   ColumnName;
    public string   OriginalColumnName;
    public string   TableName;
    public string   RealTableName;
    public string   DatabaseName;
    public Encoding Encoding;

    // protected fields
    protected int       charSetIndex;
    protected DBVersion connVersion;
    protected bool      binaryOk;

    // internal fields
    internal Driver driver;

#endregion

    [SecuritySafeCritical]
    public MySqlField (Driver driver)
    {
        this.driver      = driver;
        this.connVersion = driver.Version;
        this.MaxLength   = 1;
        this.binaryOk    = true;
#if !NETFRAMEWORK
      Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
#endif
    }

#region Properties

    public int CharacterSetIndex
    {
        get => this.charSetIndex;
        set
        {
            this.charSetIndex = value;
            this.SetFieldEncoding ();
        }
    }

    public MySqlDbType Type { get; internal set; }

    public byte Precision { get; set; }

    public byte Scale { get; set; }

    public int MaxLength { get; set; }

    public ColumnFlags Flags { get; protected set; }

    public bool IsAutoIncrement => (this.Flags & ColumnFlags.AUTO_INCREMENT) > 0;

    public bool IsNumeric => (this.Flags & ColumnFlags.NUMBER) > 0;

    public bool AllowsNull => (this.Flags & ColumnFlags.NOT_NULL) == 0;

    public bool IsUnique => (this.Flags & ColumnFlags.UNIQUE_KEY) > 0;

    public bool IsPrimaryKey => (this.Flags & ColumnFlags.PRIMARY_KEY) > 0;

    public bool IsBlob =>
        (this.Type >= MySqlDbType.TinyBlob && this.Type <= MySqlDbType.Blob) ||
        (this.Type >= MySqlDbType.TinyText && this.Type <= MySqlDbType.Text) ||
        (this.Flags & ColumnFlags.BLOB) > 0;

    public bool IsBinary => this.binaryOk && this.CharacterSetIndex == 63;

    public bool IsUnsigned => (this.Flags & ColumnFlags.UNSIGNED) > 0;

    public bool IsTextField =>
        this.Type == MySqlDbType.VarString || this.Type == MySqlDbType.VarChar || this.Type == MySqlDbType.String ||
        (this.Type == MySqlDbType.Guid && !this.driver.Settings.OldGuids);

    private int CharacterLength => this.ColumnLength / this.MaxLength;

    public List <Type> TypeConversions { get; } = new List <Type> ();

#endregion

    public void SetTypeAndFlags (MySqlDbType type, ColumnFlags flags)
    {
        this.Flags = flags;
        this.Type  = type;

        if (string.IsNullOrEmpty (this.TableName) && string.IsNullOrEmpty (this.RealTableName) && this.IsBinary && this.driver.Settings.FunctionsReturnString)
            this.CharacterSetIndex = this.driver.ConnectionCharSetIndex;

        // if our type is an unsigned number, then we need
        // to bump it up into our unsigned types
        // we're trusting that the server is not going to set the UNSIGNED
        // flag unless we are a number
        if (this.IsUnsigned)
            switch (type)
            {
                case MySqlDbType.Byte:
                    this.Type = MySqlDbType.UByte;
                    return;
                case MySqlDbType.Int16:
                    this.Type = MySqlDbType.UInt16;
                    return;
                case MySqlDbType.Int24:
                    this.Type = MySqlDbType.UInt24;
                    return;
                case MySqlDbType.Int32:
                    this.Type = MySqlDbType.UInt32;
                    return;
                case MySqlDbType.Int64:
                    this.Type = MySqlDbType.UInt64;
                    return;
            }

        if (this.IsBlob)
        {
            // handle blob to UTF8 conversion if requested.  This is only activated
            // on binary blobs
            if (this.IsBinary && this.driver.Settings.TreatBlobsAsUTF8)
            {
                bool  convertBlob  = false;
                Regex includeRegex = this.driver.Settings.GetBlobAsUTF8IncludeRegex ();
                Regex excludeRegex = this.driver.Settings.GetBlobAsUTF8ExcludeRegex ();

                if (includeRegex != null && includeRegex.IsMatch (this.ColumnName))
                    convertBlob = true;
                else if (includeRegex == null && excludeRegex != null &&
                         !excludeRegex.IsMatch (this.ColumnName))
                    convertBlob = true;

                if (convertBlob)
                {
                    this.binaryOk     = false;
                    this.Encoding     = Encoding.GetEncoding ("UTF-8");
                    this.charSetIndex = -1; // lets driver know we are in charge of encoding
                    this.MaxLength    = 4;
                }
            }

            if (!this.IsBinary)
            {
                if (type == MySqlDbType.TinyBlob)
                    this.Type = MySqlDbType.TinyText;
                else if (type == MySqlDbType.MediumBlob)
                    this.Type = MySqlDbType.MediumText;
                else if (type == MySqlDbType.Blob)
                    this.Type = MySqlDbType.Text;
                else if (type == MySqlDbType.LongBlob)
                    this.Type = MySqlDbType.LongText;
            }

            if (type == MySqlDbType.JSON)
            {
                this.binaryOk     = false;
                this.Encoding     = Encoding.GetEncoding ("UTF-8");
                this.charSetIndex = -1; // lets driver know we are in charge of encoding
                this.MaxLength    = 4;
            }
        }

        // now determine if we really should be binary
        if (this.driver.Settings.RespectBinaryFlags)
            this.CheckForExceptions ();

        if (this.Type == MySqlDbType.String && this.CharacterLength == 36 && !this.driver.Settings.OldGuids)
            this.Type = MySqlDbType.Guid;

        if (!this.IsBinary)
            return;

        if (this.driver.Settings.RespectBinaryFlags)
        {
            if (type == MySqlDbType.String)
                this.Type = MySqlDbType.Binary;
            else if (type == MySqlDbType.VarChar ||
                     type == MySqlDbType.VarString)
                this.Type = MySqlDbType.VarBinary;
        }

        if (this.CharacterSetIndex == 63)
            this.CharacterSetIndex = this.driver.ConnectionCharSetIndex;

        if (this.Type == MySqlDbType.Binary && this.ColumnLength == 16 && this.driver.Settings.OldGuids)
            this.Type = MySqlDbType.Guid;
    }

    public void AddTypeConversion (Type t)
    {
        if (this.TypeConversions.Contains (t))
            return;

        this.TypeConversions.Add (t);
    }

    private void CheckForExceptions ()
    {
        string colName = string.Empty;

        if (this.OriginalColumnName != null)
            colName = StringUtility.ToUpperInvariant (this.OriginalColumnName);

        if (colName.StartsWith ("CHAR(", StringComparison.Ordinal))
            this.binaryOk = false;
    }

    public IMySqlValue GetValueObject ()
    {
        IMySqlValue v = GetIMySqlValue (this.Type);

        if (v is MySqlByte && this.ColumnLength == 1 && this.driver.Settings.TreatTinyAsBoolean)
        {
            MySqlByte b = (MySqlByte) v;
            b.TreatAsBoolean = true;
            v                = b;
        }
        else if (v is MySqlGuid)
        {
            MySqlGuid g = (MySqlGuid) v;
            g.OldGuids = this.driver.Settings.OldGuids;
            v          = g;
        }

        return v;
    }

    public static IMySqlValue GetIMySqlValue (MySqlDbType type)
    {
        switch (type)
        {
            case MySqlDbType.Byte:  return new MySqlByte ();
            case MySqlDbType.UByte: return new MySqlUByte ();
            case MySqlDbType.Year:
            case MySqlDbType.Int16:
                return new MySqlInt16 ();
            case MySqlDbType.UInt16: return new MySqlUInt16 ();
            case MySqlDbType.Int24:
            case MySqlDbType.Int32:
                return new MySqlInt32 (type, true);
            case MySqlDbType.UInt24:
            case MySqlDbType.UInt32:
                return new MySqlUInt32 (type, true);
            case MySqlDbType.Bit:    return new MySqlBit ();
            case MySqlDbType.Int64:  return new MySqlInt64 ();
            case MySqlDbType.UInt64: return new MySqlUInt64 ();
            case MySqlDbType.Time:   return new MySqlTimeSpan ();
            case MySqlDbType.Date:
            case MySqlDbType.DateTime:
            case MySqlDbType.Newdate:
            case MySqlDbType.Timestamp:
                return new MySqlDateTime (type, true);
            case MySqlDbType.Decimal:
            case MySqlDbType.NewDecimal:
                return new MySqlDecimal ();
            case MySqlDbType.Float:  return new MySqlSingle ();
            case MySqlDbType.Double: return new MySqlDouble ();
            case MySqlDbType.Set:
            case MySqlDbType.Enum:
            case MySqlDbType.String:
            case MySqlDbType.VarString:
            case MySqlDbType.VarChar:
            case MySqlDbType.Text:
            case MySqlDbType.TinyText:
            case MySqlDbType.MediumText:
            case MySqlDbType.LongText:
            case MySqlDbType.JSON:
            case (MySqlDbType) Field_Type.NULL:
                return new MySqlString (type, true);
            case MySqlDbType.Geometry: return new MySqlGeometry (type, true);
            case MySqlDbType.Blob:
            case MySqlDbType.MediumBlob:
            case MySqlDbType.LongBlob:
            case MySqlDbType.TinyBlob:
            case MySqlDbType.Binary:
            case MySqlDbType.VarBinary:
                return new MySqlBinary (type, true);
            case MySqlDbType.Guid: return new MySqlGuid ();
            default:               throw new MySqlException ("Unknown data type");
        }
    }

    private void SetFieldEncoding ()
    {
        Dictionary <int, string> charSets = this.driver.CharacterSets;

        if (charSets == null || charSets.Count == 0 || this.CharacterSetIndex == -1)
            return;

        if (charSets [this.CharacterSetIndex] == null)
            return;

        CharacterSet cs = CharSetMap.GetCharacterSet (charSets [this.CharacterSetIndex]);
        this.MaxLength = cs.byteCount;
        this.Encoding  = CharSetMap.GetEncoding (charSets [this.CharacterSetIndex]);
    }
}
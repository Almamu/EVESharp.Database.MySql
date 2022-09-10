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

using EVESharp.Database.MySql.Types;
using System;
using System.Collections;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Reflection;
using System.Text;

namespace EVESharp.Database.MySql;

/// <summary>
/// Represents a parameter to a <see cref="MySqlCommand"/>, This class cannot be inherited.
/// </summary>
/// <remarks>
///  Parameter names are not case sensitive.
/// You can read more about it <see href="https://dev.mysql.com/doc/connector-net/en/connector-net-tutorials-parameters.html">here</see>.
/// </remarks>
[TypeConverter (typeof (MySqlParameterConverter))]
public sealed partial class MySqlParameter : DbParameter, IDbDataParameter
{
    private const int         UNSIGNED_MASK = 0x8000;
    private       object      _paramValue;
    private       string      _paramName;
    private       MySqlDbType _mySqlDbType;
    private       bool        _inferType      = true;
    private const int         GEOMETRY_LENGTH = 25;
    private       DbType      _dbType;

#region Constructors

    /// <summary>
    /// Initializes a new instance of the <see cref="MySqlParameter"/> class with the parameter name, the <see cref="MySqlParameter.MySqlDbType"/>, the size, and the source column name.
    /// </summary>
    /// <param name="parameterName">The name of the parameter to map. </param>
    /// <param name="dbType">One of the <see cref="MySqlParameter.MySqlDbType"/> values. </param>
    /// <param name="size">The length of the parameter. </param>
    /// <param name="sourceColumn">The name of the source column. </param>
    public MySqlParameter (string parameterName, MySqlDbType dbType, int size, string sourceColumn) : this (parameterName, dbType)
    {
        this.Size          = size;
        this.Direction     = ParameterDirection.Input;
        this.SourceColumn  = sourceColumn;
        this.SourceVersion = DataRowVersion.Default;
    }

    public MySqlParameter ()
    {
        this.DbType        = DbType.String;
        this.MySqlDbType   = MySqlDbType.VarChar;
        this.SourceVersion = DataRowVersion.Default;
        this.Direction     = ParameterDirection.Input;
        this._inferType    = true;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="MySqlParameter"/> class with the parameter name and a value of the new MySqlParameter.
    /// </summary>
    /// <param name="parameterName">The name of the parameter to map. </param>
    /// <param name="value">An <see cref="Object"/> that is the value of the <see cref="MySqlParameter"/>. </param>
    public MySqlParameter (string parameterName, object value) : this ()
    {
        this.ParameterName = parameterName;
        this.Value         = value;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="MySqlParameter"/> class with the parameter name and the data type.
    /// </summary>
    /// <param name="parameterName">The name of the parameter to map. </param>
    /// <param name="dbType">One of the <see cref="MySqlDbType"/> values. </param>
    public MySqlParameter (string parameterName, MySqlDbType dbType) : this (parameterName, null)
    {
        this.MySqlDbType = dbType;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="MySqlParameter"/> class with the parameter name, the <see cref="MySqlDbType"/>, and the size.
    /// </summary>
    /// <param name="parameterName">The name of the parameter to map. </param>
    /// <param name="dbType">One of the <see cref="MySqlDbType"/> values. </param>
    /// <param name="size">The length of the parameter. </param>
    public MySqlParameter (string parameterName, MySqlDbType dbType, int size) : this (parameterName, dbType)
    {
        this.Size = size;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="MySqlParameter"/> class with the parameter name, the type of the parameter, the size of the parameter, a <see cref="ParameterDirection"/>, the precision of the parameter, the scale of the parameter, the source column, a <see cref="DataRowVersion"/> to use, and the value of the parameter.
    /// </summary>
    /// <param name="parameterName">The name of the parameter to map. </param>
    /// <param name="dbType">One of the <see cref="MySqlParameter.MySqlDbType"/> values. </param>
    /// <param name="size">The length of the parameter. </param>
    /// <param name="direction">One of the <see cref="ParameterDirection"/> values. </param>
    /// <param name="isNullable">true if the value of the field can be null, otherwise false. </param>
    /// <param name="precision">The total number of digits to the left and right of the decimal point to which <see cref="MySqlParameter.Value"/> is resolved.</param>
    /// <param name="scale">The total number of decimal places to which <see cref="MySqlParameter.Value"/> is resolved. </param>
    /// <param name="sourceColumn">The name of the source column. </param>
    /// <param name="sourceVersion">One of the <see cref="DataRowVersion"/> values. </param>
    /// <param name="value">An <see cref="Object"/> that is the value of the <see cref="MySqlParameter"/>. </param>
    /// <exception cref="ArgumentException"/>
    public MySqlParameter
    (
        string         parameterName, MySqlDbType dbType,    int  size,  ParameterDirection direction,
        bool           isNullable,    byte        precision, byte scale, string             sourceColumn,
        DataRowVersion sourceVersion, object      value
    )
        : this (parameterName, dbType, size, sourceColumn)
    {
        this.Direction     = direction;
        this.IsNullable    = isNullable;
        this.Precision     = precision;
        this.Scale         = scale;
        this.Value         = value;
        this.SourceVersion = sourceVersion;
    }

    internal MySqlParameter
        (string name, MySqlDbType type, ParameterDirection dir, string col, DataRowVersion sourceVersion, object val, bool sourceColumnNullMapping)
        : this (name, type)
    {
        this.Direction               = dir;
        this.SourceColumn            = col;
        this.Value                   = val;
        this.SourceVersion           = sourceVersion;
        this.SourceColumnNullMapping = sourceColumnNullMapping;
    }

#endregion

#region Properties

    [Category ("Misc")]
    public override string ParameterName
    {
        get => this._paramName;
        set => this.SetParameterName (value);
    }

    internal MySqlParameterCollection Collection { get; set; }
    internal Encoding                 Encoding   { get; set; }

    internal bool TypeHasBeenSet => this._inferType == false;

    internal string BaseName
    {
        get
        {
            if (this.ParameterName.StartsWith ("@", StringComparison.Ordinal) || this.ParameterName.StartsWith ("?", StringComparison.Ordinal))
                return this.ParameterName.Substring (1);

            return this.ParameterName;
        }
    }

    /// <summary>
    /// Gets or sets a value indicating whether the parameter is input-only, output-only, bidirectional, or a stored procedure return value parameter.
    /// As of MySql version 4.1 and earlier, input-only is the only valid choice.
    /// </summary>
    [Category ("Data")]
    public override ParameterDirection Direction { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether the parameter accepts null values.
    /// </summary>
    [Browsable (false)]
    public override bool IsNullable { get; set; }

    /// <summary>
    /// Gets or sets the <see cref="MySqlParameter.MySqlDbType"/> of the parameter.
    /// </summary>
    [Category ("Data")]
    [DbProviderSpecificTypeProperty (true)]
    public MySqlDbType MySqlDbType
    {
        get => this._mySqlDbType;
        set
        {
            this.SetMySqlDbType (value);
            this._inferType = false;
        }
    }

    /// <summary>
    /// Gets or sets the maximum number of digits used to represent the <see cref="Value"/> property.
    /// </summary>
    [Category ("Data")]
    public override byte Precision { get; set; }

    /// <summary>
    /// Gets or sets the number of decimal places to which <see cref="Value"/> is resolved.
    /// </summary>
    [Category ("Data")]
    public override byte Scale { get; set; }

    /// <summary>
    /// Gets or sets the maximum size, in bytes, of the data within the column.
    /// </summary>
    [Category ("Data")]
    public override int Size { get; set; }

    /// <summary>
    /// Gets or sets the value of the parameter.
    /// </summary>
    [TypeConverter (typeof (StringConverter))]
    [Category ("Data")]
    public override object Value
    {
        get => this._paramValue;
        set
        {
            this._paramValue = value;
            byte [] valueAsByte   = value as byte [];
            string  valueAsString = value as string;

            if (valueAsByte != null)
                this.Size = valueAsByte.Length;
            else if (valueAsString != null)
                this.Size = valueAsString.Length;

            if (this._inferType)
                this.SetTypeFromValue ();
        }
    }

    internal IMySqlValue ValueObject { get; private set; }

    /// <summary>
    /// Returns the possible values for this parameter if this parameter is of type
    /// SET or ENUM.  Returns null otherwise.
    /// </summary>
    public IList PossibleValues { get; internal set; }

    /// <summary>
    /// Gets or sets the name of the source column that is mapped to the <see cref="System.Data.DataSet"/> and used for loading or returning the <see cref="MySqlParameter.Value"/>.
    /// </summary>
    [Category ("Data")]
    public override string SourceColumn { get; set; }

    /// <summary>
    /// Sets or gets a value which indicates whether the source column is nullable. 
    /// This allows <see cref="DbCommandBuilder"/> to correctly generate Update statements 
    /// for nullable columns. 
    /// </summary>
    public override bool SourceColumnNullMapping { get; set; }

    /// <summary>
    /// Gets or sets the <see cref="DbType"/> of the parameter.
    /// </summary>
    public override DbType DbType
    {
        get => this._dbType;
        set
        {
            this.SetDbType (value);
            this._inferType = false;
        }
    }

#endregion

    private void SetParameterName (string name)
    {
        this.Collection?.ParameterNameChanged (this, this._paramName, name);
        this._paramName = name;
    }

    /// <summary>
    /// Overridden. Gets a string containing the <see cref="ParameterName"/>.
    /// </summary>
    /// <returns></returns>
    public override string ToString ()
    {
        return this._paramName;
    }

    internal int GetPSType ()
    {
        switch (this._mySqlDbType)
        {
            case MySqlDbType.Bit:    return (int) MySqlDbType.Int64 | UNSIGNED_MASK;
            case MySqlDbType.UByte:  return (int) MySqlDbType.Byte | UNSIGNED_MASK;
            case MySqlDbType.UInt64: return (int) MySqlDbType.Int64 | UNSIGNED_MASK;
            case MySqlDbType.UInt32: return (int) MySqlDbType.Int32 | UNSIGNED_MASK;
            case MySqlDbType.UInt24: return (int) MySqlDbType.Int32 | UNSIGNED_MASK;
            case MySqlDbType.UInt16: return (int) MySqlDbType.Int16 | UNSIGNED_MASK;
            case MySqlDbType.Guid:   return (int) MySqlDbType.Guid - 600;
            case MySqlDbType.Enum:   return (int) MySqlDbType.VarChar;
            default:
                int value = (int) this._mySqlDbType;
                value = value > 255 ? value - 500 : value;
                return value;
        }
    }

    internal void Serialize (MySqlPacket packet, bool binary, MySqlConnectionStringBuilder settings)
    {
        if (!binary && (this._paramValue == null || this._paramValue == DBNull.Value))
        {
            packet.WriteStringNoNull ("NULL");
        }
        else
        {
            if (this.ValueObject.MySqlDbType == MySqlDbType.Guid)
            {
                MySqlGuid g = (MySqlGuid) this.ValueObject;
                g.OldGuids       = settings.OldGuids;
                this.ValueObject = g;
            }

            if (this.ValueObject.MySqlDbType == MySqlDbType.Geometry)
            {
                MySqlGeometry v = (MySqlGeometry) this.ValueObject;

                if (v.IsNull && this.Value != null)
                    MySqlGeometry.TryParse (this.Value.ToString (), out v);

                this.ValueObject = v;
            }

            this.ValueObject.WriteValue (packet, binary, this._paramValue, this.Size);
        }
    }

    private void SetMySqlDbType (MySqlDbType mysqlDbtype)
    {
        // JSON type is treated as VarChar because in MySQL Server since 8.0.13
        /// MYSQL_TYPE_JSON is not allowed as Item_param lacks a proper
        /// implementation for val_json.
        this._mySqlDbType = mysqlDbtype == MySqlDbType.JSON ? MySqlDbType.VarChar : mysqlDbtype;
        this.ValueObject  = MySqlField.GetIMySqlValue (this._mySqlDbType);
        this.SetDbTypeFromMySqlDbType ();
    }

    private void SetTypeFromValue ()
    {
        if (this._paramValue == null || this._paramValue == DBNull.Value)
            return;

        if (this._paramValue is Guid)
        {
            this.MySqlDbType = MySqlDbType.Guid;
        }
        else if (this._paramValue is TimeSpan)
        {
            this.MySqlDbType = MySqlDbType.Time;
        }
        else if (this._paramValue is DateTimeOffset)
        {
            this.MySqlDbType = MySqlDbType.DateTime;
        }
        else if (this._paramValue is bool)
        {
            this.MySqlDbType = MySqlDbType.Byte;
        }
        else
        {
            Type t = this._paramValue.GetType ();

            if (t.GetTypeInfo ().BaseType == typeof (Enum))
                t = t.GetTypeInfo ().GetEnumUnderlyingType ();

            switch (t.Name)
            {
                case "SByte":
                    this.MySqlDbType = MySqlDbType.Byte;
                    break;
                case "Byte":
                    this.MySqlDbType = MySqlDbType.UByte;
                    break;
                case "Int16":
                    this.MySqlDbType = MySqlDbType.Int16;
                    break;
                case "UInt16":
                    this.MySqlDbType = MySqlDbType.UInt16;
                    break;
                case "Int32":
                    this.MySqlDbType = MySqlDbType.Int32;
                    break;
                case "UInt32":
                    this.MySqlDbType = MySqlDbType.UInt32;
                    break;
                case "Int64":
                    this.MySqlDbType = MySqlDbType.Int64;
                    break;
                case "UInt64":
                    this.MySqlDbType = MySqlDbType.UInt64;
                    break;
                case "DateTime":
                    this.MySqlDbType = MySqlDbType.DateTime;
                    break;
                case "String":
                    this.MySqlDbType = MySqlDbType.VarChar;
                    break;
                case "Single":
                    this.MySqlDbType = MySqlDbType.Float;
                    break;
                case "Double":
                    this.MySqlDbType = MySqlDbType.Double;
                    break;
                case "MySqlGeometry":
                    this.MySqlDbType = MySqlDbType.Geometry;
                    break;
                case "Decimal":
                    this.MySqlDbType = MySqlDbType.Decimal;
                    break;
                case "Object":
                default:
                    this.MySqlDbType = MySqlDbType.Blob;
                    break;
            }
        }
    }

    // this method is pretty dumb but we want it to be fast.  it doesn't return size based
    // on value and type but just on the value.
    internal long EstimatedSize ()
    {
        if (this.Value == null || this.Value == DBNull.Value)
            return 4; // size of NULL

        if (this.Value is byte [])
            return ((byte []) this.Value).Length;

        if (this.Value is string)
            return ((string) this.Value).Length * 4; // account for UTF-8 (yeah I know)

        if (this.Value is decimal || this.Value is float)
            return 64;

        return 32;
    }

    /// <summary>
    /// Resets the <b>DbType</b> property to its original settings. 
    /// </summary>
    public override void ResetDbType ()
    {
        this._inferType = true;
    }

    private void SetDbTypeFromMySqlDbType ()
    {
        switch (this._mySqlDbType)
        {
            case MySqlDbType.NewDecimal:
            case MySqlDbType.Decimal:
                this._dbType = DbType.Decimal;
                break;
            case MySqlDbType.Byte:
                this._dbType = DbType.SByte;
                break;
            case MySqlDbType.UByte:
                this._dbType = DbType.Byte;
                break;
            case MySqlDbType.Int16:
                this._dbType = DbType.Int16;
                break;
            case MySqlDbType.UInt16:
                this._dbType = DbType.UInt16;
                break;
            case MySqlDbType.Int24:
            case MySqlDbType.Int32:
                this._dbType = DbType.Int32;
                break;
            case MySqlDbType.UInt24:
            case MySqlDbType.UInt32:
                this._dbType = DbType.UInt32;
                break;
            case MySqlDbType.Int64:
                this._dbType = DbType.Int64;
                break;
            case MySqlDbType.UInt64:
                this._dbType = DbType.UInt64;
                break;
            case MySqlDbType.Bit:
                this._dbType = DbType.UInt64;
                break;
            case MySqlDbType.Float:
                this._dbType = DbType.Single;
                break;
            case MySqlDbType.Double:
                this._dbType = DbType.Double;
                break;
            case MySqlDbType.Timestamp:
            case MySqlDbType.DateTime:
                this._dbType = DbType.DateTime;
                break;
            case MySqlDbType.Date:
            case MySqlDbType.Newdate:
            case MySqlDbType.Year:
                this._dbType = DbType.Date;
                break;
            case MySqlDbType.Time:
                this._dbType = DbType.Time;
                break;
            case MySqlDbType.Enum:
            case MySqlDbType.Set:
            case MySqlDbType.VarChar:
                this._dbType = DbType.String;
                break;
            case MySqlDbType.TinyBlob:
            case MySqlDbType.MediumBlob:
            case MySqlDbType.LongBlob:
            case MySqlDbType.Blob:
                this._dbType = DbType.Object;
                break;
            case MySqlDbType.String:
                this._dbType = DbType.StringFixedLength;
                break;
            case MySqlDbType.Guid:
                this._dbType = DbType.Guid;
                break;
        }
    }

    private void SetDbType (DbType dbType)
    {
        this._dbType = dbType;

        switch (this._dbType)
        {
            case DbType.Guid:
                this._mySqlDbType = MySqlDbType.Guid;
                break;

            case DbType.AnsiString:
            case DbType.String:
                this._mySqlDbType = MySqlDbType.VarChar;
                break;

            case DbType.AnsiStringFixedLength:
            case DbType.StringFixedLength:
                this._mySqlDbType = MySqlDbType.String;
                break;

            case DbType.Boolean:
            case DbType.Byte:
                this._mySqlDbType = MySqlDbType.UByte;
                break;

            case DbType.SByte:
                this._mySqlDbType = MySqlDbType.Byte;
                break;

            case DbType.Date:
                this._mySqlDbType = MySqlDbType.Date;
                break;
            case DbType.DateTime:
            case DbType.DateTimeOffset:
                this._mySqlDbType = MySqlDbType.DateTime;
                break;

            case DbType.Time:
                this._mySqlDbType = MySqlDbType.Time;
                break;
            case DbType.Single:
                this._mySqlDbType = MySqlDbType.Float;
                break;
            case DbType.Double:
                this._mySqlDbType = MySqlDbType.Double;
                break;

            case DbType.Int16:
                this._mySqlDbType = MySqlDbType.Int16;
                break;
            case DbType.UInt16:
                this._mySqlDbType = MySqlDbType.UInt16;
                break;

            case DbType.Int32:
                this._mySqlDbType = MySqlDbType.Int32;
                break;
            case DbType.UInt32:
                this._mySqlDbType = MySqlDbType.UInt32;
                break;

            case DbType.Int64:
                this._mySqlDbType = MySqlDbType.Int64;
                break;
            case DbType.UInt64:
                this._mySqlDbType = MySqlDbType.UInt64;
                break;

            case DbType.Decimal:
            case DbType.Currency:
                this._mySqlDbType = MySqlDbType.Decimal;
                break;

            case DbType.Object:
            case DbType.VarNumeric:
            case DbType.Binary:
            default:
                this._mySqlDbType = MySqlDbType.Blob;
                break;
        }

        if (this._dbType == DbType.Object)
        {
            byte [] value = this._paramValue as byte [];

            if (value != null && value.Length == GEOMETRY_LENGTH)
                this._mySqlDbType = MySqlDbType.Geometry;
        }

        this.ValueObject = MySqlField.GetIMySqlValue (this._mySqlDbType);
    }
}

internal class MySqlParameterConverter : TypeConverter
{
    public override bool CanConvertTo (ITypeDescriptorContext context, Type destinationType)
    {
        if (destinationType == typeof (System.ComponentModel.Design.Serialization.InstanceDescriptor))
            return true;

        // Always call the base to see if it can perform the conversion.
        return base.CanConvertTo (context, destinationType);
    }

    public override object ConvertTo
    (
        ITypeDescriptorContext context,
        CultureInfo            culture, object value, Type destinationType
    )
    {
        if (destinationType == typeof (System.ComponentModel.Design.Serialization.InstanceDescriptor))
        {
            ConstructorInfo ci = typeof (MySqlParameter).GetConstructor (
                new []
                {
                    typeof (string),
                    typeof (MySqlDbType),
                    typeof (int),
                    typeof (ParameterDirection),
                    typeof (bool),
                    typeof (byte),
                    typeof (byte),
                    typeof (string),
                    typeof (object)
                }
            );

            MySqlParameter p = (MySqlParameter) value;

            return new System.ComponentModel.Design.Serialization.InstanceDescriptor (
                ci, new object [] {p.ParameterName, p.DbType, p.Size, p.Direction, p.IsNullable, p.Precision, p.Scale, p.SourceColumn, p.Value}
            );
        }

        // Always call base, even if you can't convert.
        return base.ConvertTo (context, culture, value, destinationType);
    }
}
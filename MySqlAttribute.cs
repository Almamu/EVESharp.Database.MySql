// Copyright (c) 2021, 2022, Oracle and/or its affiliates.
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
using System.Reflection;

namespace EVESharp.Database.MySql;

/// <summary>
/// Represents a query attribute to a <see cref="MySqlCommand"/>.
/// </summary>
public class MySqlAttribute : ICloneable
{
    private const int         UNSIGNED_MASK = 0x8000;
    private       string      _attributeName;
    private       object      _attributeValue;
    private       MySqlDbType _mySqlDbType;
    private       int         _size;

#region Constructors

    /// <summary>
    /// Initializes a new instance of the <see cref="MySqlAttribute"/> class.
    /// </summary>
    public MySqlAttribute () { }

    /// <summary>
    /// Initializes a new instance of the <see cref="MySqlAttribute"/> class with the attribute name and its value.
    /// </summary>
    /// <param name="attributeName">Name of the attribute.</param>
    /// <param name="attributeValue">Value of the attribute.</param>
    public MySqlAttribute (string attributeName, object attributeValue)
    {
        this.AttributeName = attributeName;
        this.Value         = attributeValue;
    }

#endregion

#region Properties

    internal MySqlAttributeCollection Collection { get; set; }

    /// <summary>
    /// Name of the query attribute.
    /// </summary>
    public string AttributeName
    {
        get => this._attributeName;
        set
        {
            if (string.IsNullOrWhiteSpace (value))
                throw new ArgumentException ("'AttributeName' property can not be null or empty string.", "AttributeName");

            this._attributeName = value;
        }
    }

    /// <summary>
    /// Value of the query attribute.
    /// </summary>
    public object Value
    {
        get => this._attributeValue;
        set
        {
            this._attributeValue = value;

            if (value is byte [] valueAsByte)
                this._size = valueAsByte.Length;
            else if (value is string valueAsString)
                this._size = valueAsString.Length;

            this.SetTypeFromValue ();
        }
    }

    /// <summary>
    /// Gets or sets the <see cref="MySqlDbType"/> of the attribute.
    /// </summary>
    public MySqlDbType MySqlDbType
    {
        get => this._mySqlDbType;
        set => this.SetMySqlDbType (value);
    }

    internal IMySqlValue ValueObject { get; private set; }

#endregion

    /// <summary>
    /// Sets the MySqlDbType from the Value
    /// </summary>
    private void SetTypeFromValue ()
    {
        if (this._attributeValue == null || this._attributeValue == DBNull.Value)
            return;

        if (this._attributeValue is Guid)
        {
            this.MySqlDbType = MySqlDbType.Guid;
        }
        else if (this._attributeValue is TimeSpan)
        {
            this.MySqlDbType = MySqlDbType.Time;
        }
        else if (this._attributeValue is bool)
        {
            this.MySqlDbType = MySqlDbType.Byte;
        }
        else
        {
            Type t = this._attributeValue.GetType ();

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

    private void SetMySqlDbType (MySqlDbType mysqlDbtype)
    {
        this._mySqlDbType = mysqlDbtype == MySqlDbType.JSON ? MySqlDbType.VarChar : mysqlDbtype;
        this.ValueObject  = MySqlField.GetIMySqlValue (this._mySqlDbType);
    }

    /// <summary>
    /// Gets the value for the attribute type.
    /// </summary>
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
            default:
                int value = (int) this._mySqlDbType;
                value = value > 255 ? value - 500 : value;
                return value;
        }
    }

    /// <summary>
    /// Serialize the value of the query attribute.
    /// </summary>
    internal void Serialize (MySqlPacket packet, bool binary, MySqlConnectionStringBuilder settings)
    {
        if (!binary && (this._attributeValue == null || this._attributeValue == DBNull.Value))
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

            this.ValueObject.WriteValue (packet, binary, this._attributeValue, this._size);
        }
    }

    /// <summary>
    /// Clones this object.
    /// </summary>
    /// <returns>An object that is a clone of this object.</returns>
    public MySqlAttribute Clone ()
    {
        MySqlAttribute clone = new MySqlAttribute (this._attributeName, this._attributeValue);
        return clone;
    }

    object ICloneable.Clone ()
    {
        return this.Clone ();
    }
}
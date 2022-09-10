// Copyright (c) 2019, 2021, Oracle and/or its affiliates.
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

using EVESharp.Database.MySql;
using System;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Reflection;

namespace EVESharp.Database.MySql.Common;

internal class MySqlConnectionStringOption
{
    public MySqlConnectionStringOption
    (
        string         keyword, string         synonyms, Type baseType, object defaultValue, bool obsolete,
        SetterDelegate setter,  GetterDelegate getter
    )
    {
        this.Keyword = StringUtility.ToLowerInvariant (keyword);

        if (synonyms != null)
            this.Synonyms = StringUtility.ToLowerInvariant (synonyms).Split (',');

        this.BaseType     = baseType;
        this.Obsolete     = obsolete;
        this.DefaultValue = defaultValue;
        this.Setter       = setter;
        this.Getter       = getter;
        this.IsCustomized = true;
    }

    public MySqlConnectionStringOption
    (
        string                keyword, string                synonyms, Type baseType, object defaultValue, bool obsolete,
        ClassicSetterDelegate setter,  ClassicGetterDelegate getter
    )
    {
        this.Keyword = StringUtility.ToLowerInvariant (keyword);

        if (synonyms != null)
            this.Synonyms = StringUtility.ToLowerInvariant (synonyms).Split (',');

        this.BaseType      = baseType;
        this.Obsolete      = obsolete;
        this.DefaultValue  = defaultValue;
        this.ClassicSetter = setter;
        this.ClassicGetter = getter;
        this.IsCustomized  = true;
    }

    public MySqlConnectionStringOption (string keyword, string synonyms, Type baseType, object defaultValue, bool obsolete) :
        this (
            keyword, synonyms, baseType, defaultValue, obsolete,
            delegate (MySqlBaseConnectionStringBuilder msb, MySqlConnectionStringOption sender, object value)
            {
                sender.ValidateValue (ref value);
                msb.SetInternalValue (sender.Keyword, Convert.ChangeType (value, sender.BaseType));
            },
            (msb, sender) => msb.values [sender.Keyword]
        )
    {
        this.IsCustomized = false;
    }

#region Properties

    public Type                  BaseType      { get; private set; }
    public bool                  IsCustomized  { get; }
    public string []             Synonyms      { get; private set; }
    public bool                  Obsolete      { get; private set; }
    public string                Keyword       { get; private set; }
    public object                DefaultValue  { get; private set; }
    public SetterDelegate        Setter        { get; private set; }
    public GetterDelegate        Getter        { get; private set; }
    public ClassicSetterDelegate ClassicSetter { get; private set; }
    public ClassicGetterDelegate ClassicGetter { get; private set; }

#endregion

#region Delegates

    public delegate void SetterDelegate (MySqlBaseConnectionStringBuilder msb, MySqlConnectionStringOption sender, object value);

    public delegate object GetterDelegate (MySqlBaseConnectionStringBuilder msb, MySqlConnectionStringOption sender);

    public delegate void ClassicSetterDelegate (MySqlConnectionStringBuilder msb, MySqlConnectionStringOption sender, object value);

    public delegate object ClassicGetterDelegate (MySqlConnectionStringBuilder msb, MySqlConnectionStringOption sender);

#endregion

    public bool HasKeyword (string key)
    {
        if (this.Keyword == key)
            return true;

        if (this.Synonyms == null)
            return false;

        return this.Synonyms.Any (syn => syn == key);
    }

    public void Clean (DbConnectionStringBuilder builder)
    {
        builder.Remove (this.Keyword);

        if (this.Synonyms == null)
            return;

        foreach (string syn in this.Synonyms)
            builder.Remove (syn);
    }

    public void ValidateValue (ref object value, string keyword = null, bool isXProtocol = false)
    {
        bool b;

        if (value == null)
            return;

        string typeName  = this.BaseType.Name;
        Type   valueType = value.GetType ();

        if (valueType.Name == "String")
        {
            if (this.BaseType == valueType)
            {
                return;
            }
            else if (this.BaseType == typeof (bool))
            {
                if (string.Compare ("yes", (string) value, StringComparison.OrdinalIgnoreCase) == 0)
                    value = true;
                else if (string.Compare ("no", (string) value, StringComparison.OrdinalIgnoreCase) == 0)
                    value = false;
                else if (bool.TryParse (value.ToString (), out b))
                    value = b;
                else
                    throw new ArgumentException (string.Format (Resources.ValueNotCorrectType, value));

                return;
            }
        }

        if (typeName == "Boolean" && bool.TryParse (value.ToString (), out b))
        {
            value = b;
            return;
        }

        ulong uintVal;

        if (typeName.StartsWith ("UInt64") && ulong.TryParse (value.ToString (), NumberStyles.Any, CultureInfo.InvariantCulture, out uintVal))
        {
            value = uintVal;
            return;
        }

        uint uintVal32;

        if (typeName.StartsWith ("UInt32") && uint.TryParse (value.ToString (), NumberStyles.Any, CultureInfo.InvariantCulture, out uintVal32))
        {
            value = uintVal32;
            return;
        }

        long intVal;

        if (typeName.StartsWith ("Int64") && long.TryParse (value.ToString (), NumberStyles.Any, CultureInfo.InvariantCulture, out intVal))
        {
            value = intVal;
            return;
        }

        int intVal32;

        if (typeName.StartsWith ("Int32") && int.TryParse (value.ToString (), NumberStyles.Any, CultureInfo.InvariantCulture, out intVal32))
        {
            value = intVal32;
            return;
        }

        object objValue;
        Type   baseType = this.BaseType.GetTypeInfo ().BaseType;

        if (baseType != null && baseType.Name == "Enum" && this.ParseEnum (value.ToString (), out objValue))
        {
            value = objValue;
            return;
        }

        if (!string.IsNullOrEmpty (keyword) && isXProtocol)
            switch (keyword)
            {
                case "compression": throw new ArgumentException (string.Format (ResourcesX.CompressionInvalidValue, value));
            }

        throw new ArgumentException (string.Format (Resources.ValueNotCorrectType, value));
    }

    public void ValidateValue (ref object value, string keyword)
    {
        string typeName  = this.BaseType.Name;
        Type   valueType = value.GetType ();

        switch (keyword)
        {
            case "connect-timeout":
                if (typeName != valueType.Name && !uint.TryParse (value.ToString (), NumberStyles.Any, CultureInfo.InvariantCulture, out uint uintVal))
                    throw new FormatException (ResourcesX.InvalidConnectionTimeoutValue);

                break;
        }
    }

    private bool ParseEnum (string requestedValue, out object value)
    {
        value = null;

        try
        {
            value = Enum.Parse (this.BaseType, requestedValue, true);

            if (value != null && Enum.IsDefined (this.BaseType, value.ToString ()))
            {
                return true;
            }
            else
            {
                value = null;
                return false;
            }
        }
        catch (ArgumentException)
        {
            return false;
        }
    }
}
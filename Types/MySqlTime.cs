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

using EVESharp.Database.MySql;
using System;
using System.Globalization;

namespace EVESharp.Database.MySql.Types;

internal struct MySqlTimeSpan : IMySqlValue
{
    public MySqlTimeSpan (bool isNull)
    {
        this.IsNull = isNull;
        this.Value  = TimeSpan.MinValue;
    }

    public MySqlTimeSpan (TimeSpan val)
    {
        this.IsNull = false;
        this.Value  = val;
    }

#region IMySqlValue Members

    public bool IsNull { get; private set; }

    MySqlDbType IMySqlValue.MySqlDbType => MySqlDbType.Time;

    object IMySqlValue.Value => this.Value;

    public TimeSpan Value { get; private set; }

    Type IMySqlValue.SystemType => typeof (TimeSpan);

    string IMySqlValue.MySqlTypeName => "TIME";

    void IMySqlValue.WriteValue (MySqlPacket packet, bool binary, object val, int length)
    {
#if NET6_0
      if (val is TimeOnly)
        val = ((TimeOnly)val).ToTimeSpan();
#endif
        if (!(val is TimeSpan))
            throw new MySqlException ("Only TimeSpan objects can be serialized by MySqlTimeSpan");

        TimeSpan ts       = (TimeSpan) val;
        bool     negative = ts.TotalMilliseconds < 0;
        ts = ts.Duration ();

        if (binary)
        {
            if (ts.Milliseconds > 0)
                packet.WriteByte (12);
            else
                packet.WriteByte (8);

            packet.WriteByte ((byte) (negative ? 1 : 0));
            packet.WriteInteger (ts.Days, 4);
            packet.WriteByte ((byte) ts.Hours);
            packet.WriteByte ((byte) ts.Minutes);
            packet.WriteByte ((byte) ts.Seconds);
            int microseconds = (int) (ts.Ticks % 10_000_000) / 10;

            if (microseconds != 0)
                packet.WriteInteger (microseconds, 4);
        }
        else
        {
            string s = $"'{(negative ? "-" : "")}{ts.Days} {ts.Hours:00}:{ts.Minutes:00}:{ts.Seconds:00}.{ts.Ticks % 10000000 / 10:000000}'";

            packet.WriteStringNoNull (s);
        }
    }

    IMySqlValue IMySqlValue.ReadValue (MySqlPacket packet, long length, bool nullVal)
    {
        if (nullVal)
            return new MySqlTimeSpan (true);

        if (length >= 0)
        {
            string value = packet.ReadString (length);
            this.ParseMySql (value);
            return this;
        }

        long bufLength = packet.ReadByte ();
        int  negate    = 0;

        if (bufLength > 0)
            negate = packet.ReadByte ();

        this.IsNull = false;

        if (bufLength == 0)
        {
            this.IsNull = true;
            this.Value  = new MySqlTimeSpan ().Value;
        }
        else if (bufLength == 5)
        {
            this.Value = new TimeSpan (packet.ReadInteger (4), 0, 0, 0);
        }
        else if (bufLength == 8)
        {
            this.Value = new TimeSpan (
                packet.ReadInteger (4),
                packet.ReadByte (), packet.ReadByte (), packet.ReadByte ()
            );
        }
        else
        {
            int days         = (int) packet.ReadInteger (4);
            int hours        = (int) packet.ReadByte ();
            int minutes      = (int) packet.ReadByte ();
            int seconds      = (int) packet.ReadByte ();
            int microseconds = (int) packet.ReadInteger (4);
            this.Value = new TimeSpan (days, hours, minutes, seconds) + TimeSpan.FromTicks (microseconds * 10);
        }

        if (negate == 1)
            this.Value = this.Value.Negate ();

        return this;
    }

    void IMySqlValue.SkipValue (MySqlPacket packet)
    {
        int len = packet.ReadByte ();
        packet.Position += len;
    }

#endregion

    internal static void SetDSInfo (MySqlSchemaCollection sc)
    {
        // we use name indexing because this method will only be called
        // when GetSchema is called for the DataSourceInformation 
        // collection and then it wil be cached.
        MySqlSchemaRow row = sc.AddRow ();
        row ["TypeName"]              = "TIME";
        row ["ProviderDbType"]        = MySqlDbType.Time;
        row ["ColumnSize"]            = 0;
        row ["CreateFormat"]          = "TIME";
        row ["CreateParameters"]      = null;
        row ["DataType"]              = "System.TimeSpan";
        row ["IsAutoincrementable"]   = false;
        row ["IsBestMatch"]           = true;
        row ["IsCaseSensitive"]       = false;
        row ["IsFixedLength"]         = true;
        row ["IsFixedPrecisionScale"] = true;
        row ["IsLong"]                = false;
        row ["IsNullable"]            = true;
        row ["IsSearchable"]          = true;
        row ["IsSearchableWithLike"]  = false;
        row ["IsUnsigned"]            = false;
        row ["MaximumScale"]          = 0;
        row ["MinimumScale"]          = 0;
        row ["IsConcurrencyType"]     = DBNull.Value;
        row ["IsLiteralSupported"]    = false;
        row ["LiteralPrefix"]         = null;
        row ["LiteralSuffix"]         = null;
        row ["NativeDataType"]        = null;
    }

    public override string ToString ()
    {
        return $"{this.Value.Days} {this.Value.Hours:00}:{this.Value.Minutes:00}:{this.Value.Seconds:00}";
    }

    private void ParseMySql (string s)
    {
        string [] parts       = s.Split (':', '.');
        int       hours       = int.Parse (parts [0], CultureInfo.InvariantCulture);
        int       mins        = int.Parse (parts [1], CultureInfo.InvariantCulture);
        int       secs        = int.Parse (parts [2], CultureInfo.InvariantCulture);
        int       nanoseconds = 0;

        if (parts.Length > 3)
        {
            //if the data is saved in MySql as Time(3) the division by 1000 always returns 0, but handling the data as Time(6) the result is the expected
            parts [3]   = parts [3].PadRight (7, '0');
            nanoseconds = int.Parse (parts [3], CultureInfo.InvariantCulture);
        }

        if (hours < 0 || parts [0].StartsWith ("-", StringComparison.Ordinal))
        {
            mins        *= -1;
            secs        *= -1;
            nanoseconds *= -1;
        }

        int days = hours / 24;
        hours       = hours - days * 24;
        this.Value  = new TimeSpan (days, hours, mins, secs).Add (new TimeSpan (nanoseconds));
        this.IsNull = false;
    }
}
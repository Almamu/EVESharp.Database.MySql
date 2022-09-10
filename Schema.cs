// Copyright (c) 2013, 2019, Oracle and/or its affiliates. All rights reserved.
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
using System.Data;

namespace EVESharp.Database.MySql;

/// <summary>
/// Represents a schema and its contents.
/// </summary>
public partial class MySqlSchemaCollection
{
    private readonly List <SchemaColumn>   _columns = new List <SchemaColumn> ();
    private readonly List <MySqlSchemaRow> _rows    = new List <MySqlSchemaRow> ();
    private readonly DataTable             _table   = null;

    public MySqlSchemaCollection ()
    {
        this.Mapping         = new Dictionary <string, int> (StringComparer.OrdinalIgnoreCase);
        this.LogicalMappings = new Dictionary <int, int> ();
    }

    public MySqlSchemaCollection (string name) : this ()
    {
        this.Name = name;
    }

    public MySqlSchemaCollection (DataTable dt) : this ()
    {
        // cache the original datatable to avoid the overhead of creating again whenever possible.
        this._table = dt;
        int i = 0;

        foreach (DataColumn dc in dt.Columns)
        {
            this.Columns.Add (
                new SchemaColumn ()
                {
                    Name = dc.ColumnName,
                    Type = dc.DataType
                }
            );

            this.Mapping.Add (dc.ColumnName, i++);
            this.LogicalMappings [this.Columns.Count - 1] = this.Columns.Count - 1;
        }

        foreach (DataRow dr in dt.Rows)
        {
            MySqlSchemaRow row = new MySqlSchemaRow (this);

            for (i = 0; i < this.Columns.Count; i++)
                row [i] = dr [i];

            this.Rows.Add (row);
        }
    }

    internal Dictionary <string, int> Mapping;
    internal Dictionary <int, int>    LogicalMappings;

    /// <summary>
    /// Gets or sets the name of the schema.
    /// </summary>
    public string Name { get; set; }

    /// <summary>
    /// Gets the list of columns in the schema.
    /// </summary>
    public IList <SchemaColumn> Columns => this._columns;

    /// <summary>
    /// Gets the list of rows in the schema.
    /// </summary>
    public IList <MySqlSchemaRow> Rows => this._rows;

    internal SchemaColumn AddColumn (string name, Type t)
    {
        SchemaColumn c = new SchemaColumn
        {
            Name = name,
            Type = t
        };

        this._columns.Add (c);
        this.Mapping.Add (name, this._columns.Count - 1);
        this.LogicalMappings [this._columns.Count - 1] = this._columns.Count - 1;
        return c;
    }

    internal int ColumnIndex (string name)
    {
        int index = -1;

        for (int i = 0; i < this._columns.Count; i++)
        {
            SchemaColumn c = this._columns [i];

            if (string.Compare (c.Name, name, StringComparison.OrdinalIgnoreCase) != 0)
                continue;

            index = i;
            break;
        }

        return index;
    }

    internal void RemoveColumn (string name)
    {
        int index = this.ColumnIndex (name);

        if (index == -1)
            throw new InvalidOperationException ();

        this._columns.RemoveAt (index);

        for (int i = index; i < this.Columns.Count; i++)
            this.LogicalMappings [i] = this.LogicalMappings [i] + 1;
    }

    internal bool ContainsColumn (string name)
    {
        return this.ColumnIndex (name) >= 0;
    }

    internal MySqlSchemaRow AddRow ()
    {
        MySqlSchemaRow r = new MySqlSchemaRow (this);
        this._rows.Add (r);
        return r;
    }

    internal MySqlSchemaRow NewRow ()
    {
        MySqlSchemaRow r = new MySqlSchemaRow (this);
        return r;
    }

    internal DataTable AsDataTable ()
    {
        if (this._table != null)
            return this._table;

        DataTable dt = new DataTable (this.Name);

        foreach (SchemaColumn col in this.Columns)
            dt.Columns.Add (col.Name, col.Type);

        foreach (MySqlSchemaRow row in this.Rows)
        {
            DataRow newRow = dt.NewRow ();

            for (int i = 0; i < dt.Columns.Count; i++)
                newRow [i] = row [i] == null ? DBNull.Value : row [i];

            dt.Rows.Add (newRow);
        }

        return dt;
    }
}

/// <summary>
/// Represents a row within a schema.
/// </summary>
public class MySqlSchemaRow
{
    private Dictionary <int, object> _data;

    public MySqlSchemaRow (MySqlSchemaCollection c)
    {
        this.Collection = c;
        this.InitMetadata ();
    }

    internal void InitMetadata ()
    {
        this._data = new Dictionary <int, object> ();
    }

    internal MySqlSchemaCollection Collection { get; }

    internal object this [string s]
    {
        get => this.GetValueForName (s);
        set => this.SetValueForName (s, value);
    }

    internal object this [int i]
    {
        get
        {
            int idx = this.Collection.LogicalMappings [i];

            if (!this._data.ContainsKey (idx))
                this._data [idx] = null;

            return this._data [idx];
        }
        set => this._data [this.Collection.LogicalMappings [i]] = value;
    }

    private void SetValueForName (string colName, object value)
    {
        int index = this.Collection.Mapping [colName];
        this [index] = value;
    }

    private object GetValueForName (string colName)
    {
        int index = this.Collection.Mapping [colName];

        if (!this._data.ContainsKey (index))
            this._data [index] = null;

        return this [index];
    }

    internal void CopyRow (MySqlSchemaRow row)
    {
        if (this.Collection.Columns.Count != row.Collection.Columns.Count)
            throw new InvalidOperationException ("column count doesn't match");

        for (int i = 0; i < this.Collection.Columns.Count; i++)
            row [i] = this [i];
    }
}

/// <summary>
/// Represents a column within a schema.
/// </summary>
public class SchemaColumn
{
    /// <summary>
    /// The name of the column.
    /// </summary>
    public string Name { get; set; }

    /// <summary>
    /// The type of the column.
    /// </summary>
    public Type Type { get; set; }
}
﻿// Copyright © 2015, Oracle and/or its affiliates. All rights reserved.
//
// MySQL Connector/NET is licensed under the terms of the GPLv2
// <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most 
// MySQL Connectors. There are special exceptions to the terms and 
// conditions of the GPLv2 as it is applied to this software, see the 
// FLOSS License Exception
// <http://www.mysql.com/about/legal/licensing/foss-exception.html>.
//
// This program is free software; you can redistribute it and/or modify 
// it under the terms of the GNU General Public License as published 
// by the Free Software Foundation; version 2 of the License.
//
// This program is distributed in the hope that it will be useful, but 
// WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY 
// or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License 
// for more details.
//
// You should have received a copy of the GNU General Public License along 
// with this program; if not, write to the Free Software Foundation, Inc., 
// 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

using System;
using System.Collections.Generic;
using System.Diagnostics;
using MySql.XDevAPI.Common;
using MySql.Session;

namespace MySql.XDevAPI.Relational
{
  /// <summary>
  /// Represents a resultset that contains rows of data.  
  /// </summary>
  public class InternalRowResult : BufferingResult<Row>
  {
    internal InternalRowResult(InternalSession session) : base(session)
    {
    }

    /// <summary>
    /// The columns of this resulset
    /// </summary>
    public IReadOnlyList<Column> Columns
    {
      get { return _columns.AsReadOnly(); }
    }

    /// <summary>
    /// The rows of this resultset.  This collection will be incomplete unless all the rows have been read
    /// either by using the Next method or the Buffer method.
    /// </summary>
    public IReadOnlyList<Row> Rows
    {
      get { return _items; }
    }

    /// <summary>
    /// Allows getting the value of the column value at the current index.
    /// </summary>
    /// <param name="index">Column index</param>
    /// <returns>CLR  value at the column index</returns>
    public object this[int index]
    {
      get { return GetValue(index); }
    }

    /// <summary>
    /// Allows getting the value of the column value at the current index.
    /// </summary>
    /// <param name="index">Column index</param>
    /// <returns>CLR  value at the column index</returns>
    private object GetValue(int index)
    {
      if (_position == _items.Count)
        throw new InvalidOperationException("No data at position");
      return _items[_position][index];
    }

    /// <summary>
    /// Returns the index of the given column name
    /// </summary>
    /// <param name="name">Name of the column to find</param>
    /// <returns>Numeric index of column</returns>
    public int IndexOf(string name)
    {
      if (!NameMap.ContainsKey(name))
        throw new MySqlException("Column not found '" + name + "'");
      return NameMap[name];
    }

    protected override Row ReadItem(bool dumping)
    {
      ///TODO:  fix this
      List<byte[]> values = Protocol.ReadRow(this);
      if (values == null) return null;
      if (dumping) return new Row(this, 0);

      Debug.Assert(values.Count == _columns.Count, "Value count does not equal column count");
      Row row = new Row(this, _columns.Count);
      row.SetValues(values);
      return row;
    }
  }
}

// Copyright © 2009, 2018, Oracle and/or its affiliates. All rights reserved.
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
using System.Diagnostics;
using EVESharp.Database.MySql;
using EVESharp.Database.MySql.Types;

namespace EVESharp.Database.MySql;

internal class ResultSet
{
    private          Driver                   _driver;
    private          bool []                  _uaFieldsUsed;
    private          Dictionary <string, int> _fieldHashCi;
    private          int                      _rowIndex;
    private          bool                     _readDone;
    private          bool                     _isSequential;
    private          int                      _seqIndex;
    private readonly int                      _statementId;
    private          bool                     _cached;
    private          List <IMySqlValue []>    _cachedValues;

    public ResultSet (int affectedRows, long insertedId)
    {
        this.AffectedRows = affectedRows;
        this.InsertedId   = insertedId;
        this._readDone    = true;
    }

    public ResultSet (Driver d, int statementId, int numCols)
    {
        this.AffectedRows = -1;
        this.InsertedId   = -1;
        this._driver      = d;
        this._statementId = statementId;
        this._rowIndex    = -1;
        this.LoadColumns (numCols);
        this.IsOutputParameters = this.IsOutputParameterResultSet ();
        this.HasRows            = this.GetNextRow ();
        this._readDone          = !this.HasRows;
    }

#region Properties

    public bool HasRows { get; }

    public int Size => this.Fields?.Length ?? 0;

    public MySqlField [] Fields { get; private set; }

    public IMySqlValue [] Values { get; private set; }

    public bool IsOutputParameters { get; set; }

    public int AffectedRows { get; private set; }

    public long InsertedId { get; private set; }

    public int TotalRows { get; private set; }

    public int SkippedRows { get; private set; }

    public bool Cached
    {
        get => this._cached;
        set
        {
            this._cached = value;

            if (this._cached && this._cachedValues == null)
                this._cachedValues = new List <IMySqlValue []> ();
        }
    }

#endregion

    /// <summary>
    /// return the ordinal for the given column name
    /// </summary>
    /// <param name="name"></param>
    /// <returns></returns>
    public int GetOrdinal (string name)
    {
        int ordinal;

        // quick hash lookup using CI hash
        if (this._fieldHashCi.TryGetValue (name, out ordinal))
            return ordinal;

        // Throw an exception if the ordinal cannot be found.
        throw new IndexOutOfRangeException (string.Format (Resources.CouldNotFindColumnName, name));
    }

    /// <summary>
    /// Retrieve the value as the given column index
    /// </summary>
    /// <param name="index">The column value to retrieve</param>
    /// <returns>The value as the given column</returns>
    public IMySqlValue this [int index]
    {
        get
        {
            if (this._rowIndex < 0)
                throw new MySqlException (Resources.AttemptToAccessBeforeRead);

            // keep count of how many columns we have left to access
            this._uaFieldsUsed [index] = true;

            if (this._isSequential && index != this._seqIndex)
            {
                if (index < this._seqIndex)
                    throw new MySqlException (Resources.ReadingPriorColumnUsingSeqAccess);

                while (this._seqIndex < index - 1)
                    this._driver.SkipColumnValue (this.Values [++this._seqIndex]);

                this.Values [index] = this._driver.ReadColumnValue (index, this.Fields [index], this.Values [index]);
                this._seqIndex      = index;
            }

            return this.Values [index];
        }
    }

    private bool GetNextRow ()
    {
        bool fetched = this._driver.FetchDataRow (this._statementId, this.Size);

        if (fetched)
            this.TotalRows++;

        return fetched;
    }

    public bool NextRow (CommandBehavior behavior)
    {
        if (this._readDone)
        {
            if (this.Cached)
                return this.CachedNextRow (behavior);

            return false;
        }

        if ((behavior & CommandBehavior.SingleRow) != 0 && this._rowIndex == 0)
            return false;

        this._isSequential = (behavior & CommandBehavior.SequentialAccess) != 0;
        this._seqIndex     = -1;

        // if we are at row index >= 0 then we need to fetch the data row and load it
        if (this._rowIndex >= 0)
        {
            bool fetched = false;

            try
            {
                fetched = this.GetNextRow ();
            }
            catch (MySqlException ex)
            {
                if (ex.IsQueryAborted)
                    // avoid hanging on Close()
                    this._readDone = true;

                throw;
            }

            if (!fetched)
            {
                this._readDone = true;
                return false;
            }
        }

        if (!this._isSequential)
            this.ReadColumnData (false);

        this._rowIndex++;
        return true;
    }

    private bool CachedNextRow (CommandBehavior behavior)
    {
        if ((behavior & CommandBehavior.SingleRow) != 0 && this._rowIndex == 0)
            return false;

        if (this._rowIndex == this.TotalRows - 1)
            return false;

        this._rowIndex++;
        this.Values = this._cachedValues [this._rowIndex];
        return true;
    }

    /// <summary>
    /// Closes the current resultset, dumping any data still on the wire
    /// </summary>
    public void Close ()
    {
        if (!this._readDone)
        {
            // if we have rows but the user didn't read the first one then mark it as skipped
            if (this.HasRows && this._rowIndex == -1)
                this.SkippedRows++;

            try
            {
                while (this._driver.IsOpen && this._driver.SkipDataRow ())
                {
                    this.TotalRows++;
                    this.SkippedRows++;
                }
            }
            catch (System.IO.IOException)
            {
                // it is ok to eat IO exceptions here, we just want to 
                // close the result set
            }

            this._readDone = true;
        }
        else if (this._driver == null)
        {
            this.CacheClose ();
        }

        this._driver = null;

        if (this.Cached)
            this.CacheReset ();
    }

    private void CacheClose ()
    {
        this.SkippedRows = this.TotalRows - this._rowIndex - 1;
    }

    private void CacheReset ()
    {
        if (!this.Cached)
            return;

        this._rowIndex    = -1;
        this.AffectedRows = -1;
        this.InsertedId   = -1;
        this.SkippedRows  = 0;
    }

    public bool FieldRead (int index)
    {
        Debug.Assert (this.Size > index);
        return this._uaFieldsUsed [index];
    }

    public void SetValueObject (int i, IMySqlValue valueObject)
    {
        Debug.Assert (this.Values != null);
        Debug.Assert (i < this.Values.Length);
        this.Values [i] = valueObject;
    }

    private bool IsOutputParameterResultSet ()
    {
        if (this._driver.HasStatus (ServerStatusFlags.OutputParameters))
            return true;

        if (this.Fields.Length == 0)
            return false;

        for (int x = 0; x < this.Fields.Length; x++)
            if (!this.Fields [x].ColumnName.StartsWith ("@" + StoredProcedure.ParameterPrefix, StringComparison.OrdinalIgnoreCase))
                return false;

        return true;
    }

    /// <summary>
    /// Loads the column metadata for the current resultset
    /// </summary>
    private void LoadColumns (int numCols)
    {
        this.Fields = this._driver.GetColumns (numCols);

        this.Values        = new IMySqlValue[numCols];
        this._uaFieldsUsed = new bool[numCols];
        this._fieldHashCi  = new Dictionary <string, int> (StringComparer.OrdinalIgnoreCase);

        for (int i = 0; i < this.Fields.Length; i++)
        {
            string columnName = this.Fields [i].ColumnName;

            if (!this._fieldHashCi.ContainsKey (columnName))
                this._fieldHashCi.Add (columnName, i);

            this.Values [i] = this.Fields [i].GetValueObject ();
        }
    }

    private void ReadColumnData (bool outputParms)
    {
        for (int i = 0; i < this.Size; i++)
            this.Values [i] = this._driver.ReadColumnValue (i, this.Fields [i], this.Values [i]);

        // if we are caching then we need to save a copy of this row of data values
        if (this.Cached)
            this._cachedValues.Add ((IMySqlValue []) this.Values.Clone ());

        // we don't need to worry about caching the following since you won't have output
        // params with TableDirect commands
        if (!outputParms)
            return;

        bool rowExists = this._driver.FetchDataRow (this._statementId, this.Fields.Length);
        this._rowIndex = 0;

        if (rowExists)
            throw new MySqlException (Resources.MoreThanOneOPRow);
    }
}
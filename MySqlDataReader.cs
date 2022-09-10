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
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading;
using EVESharp.Database.MySql;

namespace EVESharp.Database.MySql;

/// <summary>
///  Provides a means of reading a forward-only stream of rows from a MySQL database. This class cannot be inherited.
/// </summary>
/// <remarks>
///  <para>
///    To create a <see cref="MySqlDataReader"/>, you must call the <see cref="MySqlCommand.ExecuteReader()"/>
///    method of the <see cref="MySqlCommand"/> object, rather than directly using a constructor.
///  </para>
///  <para>
///    While the <see cref="MySqlDataReader"/> is in use, the associated <see cref="MySqlConnection"/>
///    is busy serving the <see cref="MySqlDataReader"/>, and no other operations can be performed
///    on the <B>MySqlConnection</B> other than closing it. This is the case until the
///    <see cref="Close"/> method of the <see cref="MySqlDataReader"/> is called.
///  </para>
///  <para>
///    <see cref="IsClosed"/> and <see cref="RecordsAffected"/>
///    are the only properties that you can call after the <see cref="MySqlDataReader"/> is
///    closed. Though the <see cref="RecordsAffected"/> property may be accessed at any time
///    while the <see cref="MySqlDataReader"/> exists, always call <B>Close</B> before returning
///    the value of <see cref="RecordsAffected"/> to ensure an accurate return value.
///  </para>
///  <para>
///    For optimal performance, <see cref="MySqlDataReader"/> avoids creating
///    unnecessary objects or making unnecessary copies of data. As a result, multiple calls
///    to methods such as <see cref="MySqlDataReader.GetValue"/> return a reference to the
///    same object. Use caution if you are modifying the underlying value of the objects
///    returned by methods such as <see cref="GetValue"/>.
///  </para>
/// </remarks>
public sealed partial class MySqlDataReader : DbDataReader, IDataReader, IDataRecord, IDisposable
{
    // The DataReader should always be open when returned to the user.
    private bool _isOpen = true;

    internal long   affectedRows;
    internal Driver driver;

    // Used in special circumstances with stored procs to avoid exceptions from DbDataAdapter
    // If set, AffectedRows returns -1 instead of 0.
    private readonly bool _disableZeroAffectedRows;

    /* 
     * Keep track of the connection in order to implement the
     * CommandBehavior.CloseConnection flag. A null reference means
     * normal behavior (do not automatically close).
     */
    private MySqlConnection _connection;

    /*
     * Because the user should not be able to directly create a 
     * DataReader object, the constructors are
     * marked as internal.
     */
    internal MySqlDataReader (MySqlCommand cmd, PreparableStatement statement, CommandBehavior behavior)
    {
        this.Command         = cmd;
        this._connection     = this.Command.Connection;
        this.CommandBehavior = behavior;
        this.driver          = this._connection.driver;
        this.affectedRows    = -1;
        this.Statement       = statement;

        if (cmd.CommandType == CommandType.StoredProcedure
            && cmd.UpdatedRowSource == UpdateRowSource.FirstReturnedRecord
           )
            this._disableZeroAffectedRows = true;
    }

#region Properties

    internal PreparableStatement Statement { get; }

    internal MySqlCommand Command { get; private set; }

    internal ResultSet ResultSet { get; private set; }

    internal CommandBehavior CommandBehavior { get; private set; }

    /// <summary>
    /// Gets the number of columns in the current row.
    /// </summary>
    /// <returns>The number of columns in the current row.</returns>
    public override int FieldCount => this.ResultSet?.Size ?? 0;

    /// <summary>
    /// Gets a value indicating whether the MySqlDataReader contains one or more rows.
    /// </summary>
    /// <returns>true if the <see cref="MySqlDataReader"/> contains one or more rows; otherwise false.</returns>
    public override bool HasRows => this.ResultSet?.HasRows ?? false;

    /// <summary>
    /// Gets a value indicating whether the data reader is closed.
    /// </summary>
    /// <returns>true if the <see cref="MySqlDataReader"/> is closed; otherwise false.</returns>
    public override bool IsClosed => !this._isOpen;

    /// <summary>
    /// Gets the number of rows changed, inserted, or deleted by execution of the SQL statement.
    /// </summary>
    /// <returns>The number of rows changed, inserted, or deleted. 
    /// -1 for SELECT statements; 0 if no rows were affected or the statement failed.</returns>
    public override int RecordsAffected
    {
        // RecordsAffected returns the number of rows affected in batch
        // statments from insert/delete/update statments.  This property
        // is not completely accurate until .Close() has been called.
        get
        {
            if (!this._disableZeroAffectedRows)
                return (int) this.affectedRows;

            // In special case of updating stored procedure called from 
            // within data adapter, we return -1 to avoid exceptions 
            // (s. Bug#54895)
            if (this.affectedRows == 0)
                return -1;

            return (int) this.affectedRows;
        }
    }

    /// <summary>
    /// Overloaded. Gets the value of a column in its native format.
    /// In C#, this property is the indexer for the <see cref="MySqlDataReader"/> class.
    /// </summary>
    /// <returns>The value of the specified column.</returns>
    public override object this [int i] => this.GetValue (i);

    /// <summary>
    /// Gets the value of a column in its native format.
    ///	[C#] In C#, this property is the indexer for the <see cref="MySqlDataReader"/> class.
    /// </summary>
    /// <returns>The value of the specified column.</returns>
    public override object this [string name] => this [this.GetOrdinal (name)];

    /// <summary>
    /// Gets a value indicating the depth of nesting for the current row.  This method is not 
    /// supported currently and always returns 0.
    /// </summary>
    /// <returns>The depth of nesting for the current row.</returns>
    public override int Depth => 0;

#endregion

    /// <summary>
    /// Closes the MySqlDataReader object.
    /// </summary>
    public override void Close ()
    {
        if (!this._isOpen)
            return;

        bool            shouldCloseConnection = (this.CommandBehavior & CommandBehavior.CloseConnection) != 0;
        CommandBehavior originalBehavior      = this.CommandBehavior;

        // clear all remaining resultsets
        try
        {
            // Temporarily change to Default behavior to allow NextResult to finish properly.
            if (!originalBehavior.Equals (CommandBehavior.SchemaOnly))
                this.CommandBehavior = CommandBehavior.Default;

            while (this.NextResult ()) { }
        }
        catch (MySqlException ex)
        {
            // Ignore aborted queries
            if (!ex.IsQueryAborted)
            {
                // ignore IO exceptions.
                // We are closing or disposing reader, and  do not
                // want exception to be propagated to used. If socket is
                // is closed on the server side, next query will run into
                // IO exception. If reader is closed by GC, we also would 
                // like to avoid any exception here. 
                bool isIOException = false;

                for (Exception exception = ex;
                     exception != null;
                     exception = exception.InnerException)
                    if (exception is IOException)
                    {
                        isIOException = true;
                        break;
                    }

                if (!isIOException)
                    // Ordinary exception (neither IO nor query aborted)
                    throw;
            }
        }
        catch (IOException)
        {
            // eat, on the same reason we eat IO exceptions wrapped into 
            // MySqlExceptions reasons, described above.
        }
        finally
        {
            // always ensure internal reader is null (Bug #55558)
            this._connection.Reader = null;
            this.CommandBehavior    = originalBehavior;
        }

        // we now give the command a chance to terminate.  In the case of
        // stored procedures it needs to update out and inout parameters
        this.Command.Close (this);
        this.CommandBehavior = CommandBehavior.Default;

        if (this.Command.Canceled && this._connection.driver.Version.isAtLeast (5, 1, 0))
            // Issue dummy command to clear kill flag
            this.ClearKillFlag ();

        if (shouldCloseConnection)
            this._connection.Close ();

        this.Command             = null;
        this._connection.IsInUse = false;
        this._connection         = null;
        this._isOpen             = false;
    }

#region TypeSafe Accessors

    /// <summary>
    /// Gets the value of the specified column as a Boolean.
    /// </summary>
    /// <param name="name">The column name.</param>
    /// <returns>The value of the specified column.</returns>
    public bool GetBoolean (string name)
    {
        return this.GetBoolean (this.GetOrdinal (name));
    }

    /// <summary>
    /// Gets the value of the specified column as a Boolean.
    /// </summary>
    /// <param name="i">The zero-based column ordinal.</param>
    /// <returns>The value of the specified column.</returns>
    public override bool GetBoolean (int i)
    {
        object asValue = this.GetValue (i);
        int    numericValue;

        if (int.TryParse (asValue as string, out numericValue))
            return Convert.ToBoolean (numericValue);

        return Convert.ToBoolean (asValue);
    }

    /// <summary>
    /// Gets the value of the specified column as a byte.
    /// </summary>
    /// <param name="name">The column name.</param>
    /// <returns>The value of the specified column.</returns>
    public byte GetByte (string name)
    {
        return this.GetByte (this.GetOrdinal (name));
    }

    /// <summary>
    /// Gets the value of the specified column as a byte.
    /// </summary>
    /// <param name="i">The zero-based column ordinal.</param>
    /// <returns>The value of the specified column.</returns>
    public override byte GetByte (int i)
    {
        IMySqlValue v = this.GetFieldValue (i, false);

        if (v is MySqlUByte)
            return ((MySqlUByte) v).Value;
        else
            return (byte) ((MySqlByte) v).Value;
    }

    /// <summary>
    /// Gets the value of the specified column as a sbyte.
    /// </summary>
    /// <param name="name">The column name.</param>
    /// <returns>The value of the specified column.</returns>
    public sbyte GetSByte (string name)
    {
        return this.GetSByte (this.GetOrdinal (name));
    }

    /// <summary>
    /// Gets the value of the specified column as a sbyte.
    /// </summary>
    /// <param name="i">The zero-based column ordinal.</param>
    /// <returns>The value of the specified column.</returns>
    public sbyte GetSByte (int i)
    {
        IMySqlValue v = this.GetFieldValue (i, false);

        if (v is MySqlByte)
            return ((MySqlByte) v).Value;
        else
            return (sbyte) ((MySqlByte) v).Value;
    }

    /// <summary>
    /// Reads a stream of bytes from the specified column offset into the buffer an array starting at the given buffer offset.
    /// </summary>
    /// <param name="i">The zero-based column ordinal.</param>
    /// <param name="fieldOffset">The index within the field from which to begin the read operation.</param>
    /// <param name="buffer">The buffer into which to read the stream of bytes.</param>
    /// <param name="bufferoffset">The index for buffer to begin the read operation.</param>
    /// <param name="length">The maximum length to copy into the buffer.</param>
    /// <returns>The actual number of bytes read.</returns>
    public override long GetBytes (int i, long fieldOffset, byte [] buffer, int bufferoffset, int length)
    {
        if (i >= this.FieldCount)
            this.Throw (new IndexOutOfRangeException ());

        IMySqlValue val = this.GetFieldValue (i, false);

        if (!(val is MySqlBinary) && !(val is MySqlGuid))
            this.Throw (new MySqlException ("GetBytes can only be called on binary or guid columns"));

        byte [] bytes = null;

        if (val is MySqlBinary)
            bytes = ((MySqlBinary) val).Value;
        else
            bytes = ((MySqlGuid) val).Bytes;

        if (buffer == null)
            return bytes.Length;

        if (bufferoffset >= buffer.Length || bufferoffset < 0)
            this.Throw (new IndexOutOfRangeException ("Buffer index must be a valid index in buffer"));

        if (buffer.Length < bufferoffset + length)
            this.Throw (new ArgumentException ("Buffer is not large enough to hold the requested data"));

        if (fieldOffset < 0 ||
            ((ulong) fieldOffset >= (ulong) bytes.Length && (ulong) bytes.Length > 0))
            this.Throw (new IndexOutOfRangeException ("Data index must be a valid index in the field"));

        // adjust the length so we don't run off the end
        if ((ulong) bytes.Length < (ulong) (fieldOffset + length))
            length = (int) ((ulong) bytes.Length - (ulong) fieldOffset);

        Buffer.BlockCopy (bytes, (int) fieldOffset, buffer, (int) bufferoffset, (int) length);

        return length;
    }

    private object ChangeType (IMySqlValue value, int fieldIndex, Type newType)
    {
        this.ResultSet.Fields [fieldIndex].AddTypeConversion (newType);
        return Convert.ChangeType (value.Value, newType, CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// Gets the value of the specified column as a single character.
    /// </summary>
    /// <param name="name">The column name.</param>
    /// <returns>The value of the specified column.</returns>
    public char GetChar (string name)
    {
        return this.GetChar (this.GetOrdinal (name));
    }

    /// <summary>
    /// Gets the value of the specified column as a single character.
    /// </summary>
    /// <param name="i">The zero-based column ordinal.</param>
    /// <returns>The value of the specified column.</returns>
    public override char GetChar (int i)
    {
        string s = this.GetString (i);
        return s [0];
    }

    /// <summary>
    /// Reads a stream of characters from the specified column offset into the buffer as an array starting at the given buffer offset.
    /// </summary>
    /// <param name="i">The zero-based column ordinal.</param>
    /// <param name="fieldoffset">The index within the row from which to begin the read operation.</param>
    /// <param name="buffer">The buffer into which to copy the data.</param>
    /// <param name="bufferoffset">The index with the buffer to which the data will be copied.</param>
    /// <param name="length">The maximum number of characters to read.</param>
    /// <returns>The actual number of characters read.</returns>
    public override long GetChars (int i, long fieldoffset, char [] buffer, int bufferoffset, int length)
    {
        if (i >= this.FieldCount)
            this.Throw (new IndexOutOfRangeException ());

        string valAsString = this.GetString (i);

        if (buffer == null)
            return valAsString.Length;

        if (bufferoffset >= buffer.Length || bufferoffset < 0)
            this.Throw (new IndexOutOfRangeException ("Buffer index must be a valid index in buffer"));

        if (buffer.Length < bufferoffset + length)
            this.Throw (new ArgumentException ("Buffer is not large enough to hold the requested data"));

        if (fieldoffset < 0 || fieldoffset >= valAsString.Length)
            this.Throw (new IndexOutOfRangeException ("Field offset must be a valid index in the field"));

        if (valAsString.Length < length)
            length = valAsString.Length;

        valAsString.CopyTo ((int) fieldoffset, buffer, bufferoffset, length);
        return length;
    }

    /// <summary>
    /// Gets the name of the source data type.
    /// </summary>
    /// <param name="i">The zero-based column ordinal.</param>
    /// <returns>A string representing the name of the data type.</returns>
    public override string GetDataTypeName (int i)
    {
        if (!this._isOpen)
            this.Throw (new Exception ("No current query in data reader"));

        if (i >= this.FieldCount)
            this.Throw (new IndexOutOfRangeException ());

        // return the name of the type used on the backend
        IMySqlValue v = this.ResultSet.Values [i];
        return v.MySqlTypeName;
    }

    /// <summary>
    ///  Gets the value of the specified column as a <see cref="MySqlDateTime"/> object.
    /// </summary>
    /// <remarks>
    ///  <para>No conversions are performed; therefore, the data retrieved must already be a <see cref="DateTime"/> object.</para>
    ///  <para>Call IsDBNull to check for null values before calling this method.</para>
    /// </remarks>
    /// <param name="column">The column name.</param>
    /// <returns>The value of the specified column.</returns>
    public MySqlDateTime GetMySqlDateTime (string column)
    {
        return this.GetMySqlDateTime (this.GetOrdinal (column));
    }

    /// <summary>
    ///  Gets the value of the specified column as a <see cref="MySqlDateTime"/> object.
    /// </summary>
    /// <remarks>
    ///  <para>No conversions are performed; therefore, the data retrieved must already be a <see cref="DateTime"/> object.</para>
    ///  <para>Call IsDBNull to check for null values before calling this method.</para>
    /// </remarks>
    /// <param name="column">The zero-based column ordinal.</param>
    /// <returns>The value of the specified column.</returns>
    public MySqlDateTime GetMySqlDateTime (int column)
    {
        return (MySqlDateTime) this.GetFieldValue (column, true);
    }

    /// <summary>
    ///  Gets the value of the specified column as a <see cref="DateTime"/> object.
    /// </summary>
    /// <remarks>
    ///  <para>No conversions are performed; therefore, the data retrieved must already be a <see cref="DateTime"/> object.</para>
    ///  <para>Call <see cref="IsDBNull"/> to check for null values before calling this method.</para>
    ///  <note>
    ///    <para>
    ///      MySql allows date columns to contain the value '0000-00-00' and datetime
    ///      columns to contain the value '0000-00-00 00:00:00'.  The DateTime structure cannot contain
    ///      or represent these values.  To read a datetime value from a column that might
    ///      contain zero values, use <see cref="GetMySqlDateTime(int)"/>.
    ///    </para>
    ///    <para>
    ///      The behavior of reading a zero datetime column using this method is defined by the
    ///      <i>ZeroDateTimeBehavior</i> connection string option.  For more information on this option,
    ///      please refer to <see cref="MySqlConnection.ConnectionString"/>.
    ///    </para>
    ///  </note>
    /// </remarks>
    /// <param name="column">The column name.</param>
    /// <returns>The value of the specified column.</returns>
    public DateTime GetDateTime (string column)
    {
        return this.GetDateTime (this.GetOrdinal (column));
    }

    /// <summary>
    ///  Gets the value of the specified column as a <see cref="DateTime"/> object.
    /// </summary>
    /// <remarks>
    ///  <para>No conversions are performed; therefore, the data retrieved must already be a <see cref="DateTime"/> object.</para>
    ///  <para>Call <see cref="IsDBNull"/> to check for null values before calling this method.</para>
    ///  <note>
    ///    <para>
    ///      MySql allows date columns to contain the value '0000-00-00' and datetime
    ///      columns to contain the value '0000-00-00 00:00:00'.  The DateTime structure cannot contain
    ///      or represent these values.  To read a datetime value from a column that might
    ///      contain zero values, use <see cref="GetMySqlDateTime(int)"/>.
    ///    </para>
    ///    <para>
    ///      The behavior of reading a zero datetime column using this method is defined by the
    ///      <i>ZeroDateTimeBehavior</i> connection string option.  For more information on this option,
    ///      please refer to <see cref="MySqlConnection.ConnectionString"/>.
    ///    </para>
    ///  </note>
    /// </remarks>
    /// <param name="i">The zero-based column ordinal.</param>
    /// <returns>The value of the specified column.</returns>
    public override DateTime GetDateTime (int i)
    {
        IMySqlValue   val = this.GetFieldValue (i, true);
        MySqlDateTime dt;

        if (val is MySqlDateTime)
        {
            dt = (MySqlDateTime) val;
        }
        else
        {
            // we need to do this because functions like date_add return string
            string s = this.GetString (i);
            dt = MySqlDateTime.Parse (s);
        }

        dt.TimezoneOffset = this.driver.timeZoneOffset;

        if (this._connection.Settings.ConvertZeroDateTime && !dt.IsValidDateTime)
            return DateTime.MinValue;
        else
            return dt.GetDateTime ();
    }

    /// <summary>
    /// Gets the value of the specified column as a <see cref="MySqlDecimal"/>.
    /// </summary>
    /// <param name="column">The name of the colum.</param>
    /// <returns>The value of the specified column as a <see cref="MySqlDecimal"/>.</returns>
    public MySqlDecimal GetMySqlDecimal (string column)
    {
        return this.GetMySqlDecimal (this.GetOrdinal (column));
    }

    /// <summary>
    /// Gets the value of the specified column as a <see cref="MySqlDecimal"/>.
    /// </summary>
    /// <param name="i">The index of the colum.</param>
    /// <returns>The value of the specified column as a <see cref="MySqlDecimal"/>.</returns>
    public MySqlDecimal GetMySqlDecimal (int i)
    {
        return (MySqlDecimal) this.GetFieldValue (i, false);
    }

    /// <summary>
    ///  Gets the value of the specified column as a <see cref="Decimal"/> object.
    /// </summary>
    /// <remarks>
    ///  <para>No conversions are performed; therefore, the data retrieved must already be a <see cref="Decimal"/> object.</para>
    ///  <para>Call <see cref="IsDBNull"/> to check for null values before calling this method.</para>
    /// </remarks>
    /// <param name="column">The column name.</param>
    /// <returns>The value of the specified column.</returns>
    public decimal GetDecimal (string column)
    {
        return this.GetDecimal (this.GetOrdinal (column));
    }

    /// <summary>
    ///  Gets the value of the specified column as a <see cref="Decimal"/> object.
    /// </summary>
    /// <remarks>
    ///  <para>No conversions are performed; therefore, the data retrieved must already be a <see cref="Decimal"/> object.</para>
    ///  <para>Call <see cref="IsDBNull"/> to check for null values before calling this method.</para>
    /// </remarks>
    /// <param name="i">The zero-based column ordinal</param>
    /// <returns>The value of the specified column.</returns>
    public override decimal GetDecimal (int i)
    {
        IMySqlValue v = this.GetFieldValue (i, true);

        if (v is MySqlDecimal)
            return ((MySqlDecimal) v).Value;

        return Convert.ToDecimal (v.Value);
    }

    /// <summary>Gets the value of the specified column as a double-precision floating point number.</summary>
    /// <remarks>
    ///  <para>No conversions are performed; therefore, the data retrieved must already be a <see cref="double"/> object.</para>
    ///  <para>Call <see cref="IsDBNull"/> to check for null values before calling this method.</para>
    /// </remarks>
    /// <param name="column">The column name.</param>
    /// <returns>The value of the specified column.</returns>
    public double GetDouble (string column)
    {
        return this.GetDouble (this.GetOrdinal (column));
    }

    /// <summary>Gets the value of the specified column as a double-precision floating point number.</summary>
    /// <remarks>
    ///  <para>No conversions are performed; therefore, the data retrieved must already be a <see cref="double"/> object.</para>
    ///  <para>Call <see cref="IsDBNull"/> to check for null values before calling this method.</para>
    /// </remarks>
    /// <param name="i">The zero-based column ordinal.</param>
    /// <returns>The value of the specified column.</returns>
    public override double GetDouble (int i)
    {
        IMySqlValue v = this.GetFieldValue (i, true);

        if (v is MySqlDouble)
            return ((MySqlDouble) v).Value;

        return Convert.ToDouble (v.Value);
    }

    /// <summary>
    /// Gets the Type that is the data type of the object.
    /// </summary>
    /// <param name="column">The column name.</param>
    /// <returns>The data type of the specified column.</returns>
    public Type GetFieldType (string column)
    {
        return this.GetFieldType (this.GetOrdinal (column));
    }

    /// <summary>
    /// Gets the Type that is the data type of the object.
    /// </summary>
    /// <param name="i">The zero-based column ordinal.</param>
    /// <returns>The data type of the specified column.</returns>
    public override Type GetFieldType (int i)
    {
        if (!this._isOpen)
            this.Throw (new Exception ("No current query in data reader"));

        if (i >= this.FieldCount)
            this.Throw (new IndexOutOfRangeException ());

        // we have to use the values array directly because we can't go through
        // GetValue
        IMySqlValue v = this.ResultSet.Values [i];

        if (v is MySqlDateTime)
        {
            if (!this._connection.Settings.AllowZeroDateTime)
                return typeof (DateTime);

            return typeof (MySqlDateTime);
        }

        return v.SystemType;
    }

    /// <summary>
    ///  Gets the value of the specified column as a single-precision floating point number.
    /// </summary>
    /// <remarks>
    ///  <para> No conversions are performed; therefore, the data retrieved must already be a <see cref="float"/> object.</para>
    ///  <para> Call <see cref="IsDBNull"/> to check for null values before calling this method. </para>
    /// </remarks>
    /// <param name="column">The column name.</param>
    /// <returns>The value of the specified column.</returns>
    public float GetFloat (string column)
    {
        return this.GetFloat (this.GetOrdinal (column));
    }

    /// <summary>
    ///  Gets the value of the specified column as a single-precision floating point number.
    /// </summary>
    /// <remarks>
    ///  <para> No conversions are performed; therefore, the data retrieved must already be a <see cref="float"/> object.</para>
    ///  <para> Call <see cref="IsDBNull"/> to check for null values before calling this method.</para>
    /// </remarks>
    /// <param name="i">The zero-based column ordinal.</param>
    /// <returns>The value of the specified column.</returns>
    public override float GetFloat (int i)
    {
        IMySqlValue v = this.GetFieldValue (i, true);

        if (v is MySqlSingle)
            return ((MySqlSingle) v).Value;

        return Convert.ToSingle (v.Value);
    }

    /// <summary>
    /// Gets the body definition of a routine.
    /// </summary>
    /// <param name="column">The column name.</param>
    /// <returns>The definition of the routine.</returns>
    public string GetBodyDefinition (string column)
    {
        object value = this.GetValue (this.GetOrdinal (column));

        if (value.GetType ().FullName.Equals ("System.Byte[]"))
            return this.GetString (column);
        else
            return this.GetValue (this.GetOrdinal (column)).ToString ();
    }

    /// <summary>
    /// Gets the value of the specified column as a globally-unique identifier(GUID).
    /// </summary>
    /// <param name="column">The name of the column.</param>
    /// <returns>The value of the specified column.</returns>
    public Guid GetGuid (string column)
    {
        return this.GetGuid (this.GetOrdinal (column));
    }

    /// <summary>
    /// Gets the value of the specified column as a globally-unique identifier(GUID).
    /// </summary>
    /// <param name="i">The zero-based column ordinal.</param>
    /// <returns>The value of the specified column.</returns>
    public override Guid GetGuid (int i)
    {
        object v = this.GetValue (i);

        if (v is Guid)
            return (Guid) v;

        if (v is string)
            return new Guid (v as string);

        if (v is byte [])
        {
            byte [] bytes = (byte []) v;

            if (bytes.Length == 16)
                return new Guid (bytes);
        }

        this.Throw (new MySqlException (Resources.ValueNotSupportedForGuid));
        return Guid.Empty; // just to silence compiler
    }

    /// <summary>Gets the value of the specified column as a 16-bit signed integer.</summary>
    /// <remarks>
    ///  <para>No conversions are performed; therefore, the data retrieved must already be a <see cref="Int16"/> value.</para>
    ///  <para>Call <see cref="IsDBNull"/> to check for null values before calling this method.</para>
    /// </remarks>
    /// <param name="column">The column name.</param>
    /// <returns>The value of the specified column.</returns>
    public short GetInt16 (string column)
    {
        return this.GetInt16 (this.GetOrdinal (column));
    }

    /// <summary>Gets the value of the specified column as a 16-bit signed integer.</summary>
    /// <remarks>
    ///  <para>No conversions are performed; therefore, the data retrieved must already be a <see cref="Int16"/> value.</para>
    ///  <para>Call <see cref="IsDBNull"/> to check for null values before calling this method.</para>
    /// </remarks>
    /// <param name="i">The zero-based column ordinal.</param>
    /// <returns>The value of the specified column.</returns>
    public override short GetInt16 (int i)
    {
        IMySqlValue v = this.GetFieldValue (i, true);

        if (v is MySqlInt16)
            return ((MySqlInt16) v).Value;

        return (short) this.ChangeType (v, i, typeof (short));
    }

    /// <summary>Gets the value of the specified column as a 32-bit signed integer.</summary>
    /// <remarks>
    ///  <para>No conversions are performed; therefore, the data retrieved must already be a <see cref="Int32"/> value.</para>
    ///  <para>Call <see cref="IsDBNull"/> to check for null values before calling this method.</para>
    /// </remarks>
    /// <param name="column">The column name.</param>
    /// <returns>The value of the specified column.</returns>
    public int GetInt32 (string column)
    {
        return this.GetInt32 (this.GetOrdinal (column));
    }

    /// <summary>Gets the value of the specified column as a 32-bit signed integer.</summary>
    /// <remarks>
    ///  <para>No conversions are performed; therefore, the data retrieved must already be a <see cref="Int32"/> value.</para>
    ///  <para>Call <see cref="IsDBNull"/> to check for null values before calling this method.</para>
    /// </remarks>
    /// <param name="i">The zero-based column ordinal.</param>
    /// <returns>The value of the specified column.</returns>
    public override int GetInt32 (int i)
    {
        IMySqlValue v = this.GetFieldValue (i, true);

        if (v is MySqlInt32)
            return ((MySqlInt32) v).Value;

        return (int) this.ChangeType (v, i, typeof (int));
    }

    /// <summary>Gets the value of the specified column as a 64-bit signed integer.</summary>
    /// <remarks>
    ///  <para>No conversions are performed; therefore, the data retrieved must already be a <see cref="Int64"/> value.</para>
    ///  <para>Call <see cref="IsDBNull"/> to check for null values before calling this method.</para>
    /// </remarks>
    /// <param name="column">The column name.</param>
    /// <returns>The value of the specified column.</returns>
    public long GetInt64 (string column)
    {
        return this.GetInt64 (this.GetOrdinal (column));
    }

    /// <summary>Gets the value of the specified column as a 64-bit signed integer.</summary>
    /// <remarks>
    ///  <para>No conversions are performed; therefore, the data retrieved must already be a <see cref="Int64"/> value.</para>
    ///  <para>Call <see cref="IsDBNull"/> to check for null values before calling this method.</para>
    /// </remarks>
    /// <param name="i">The zero-based column ordinal.</param>
    /// <returns>The value of the specified column.</returns>
    public override long GetInt64 (int i)
    {
        IMySqlValue v = this.GetFieldValue (i, true);

        if (v is MySqlInt64)
            return ((MySqlInt64) v).Value;

        return (long) this.ChangeType (v, i, typeof (long));
    }

    /// <summary>
    /// Gets the name of the specified column.
    /// </summary>
    /// <param name="i">The zero-based column ordinal.</param>
    /// <returns>The name of the specified column.</returns>
    public override string GetName (int i)
    {
        if (!this._isOpen)
            this.Throw (new Exception ("No current query in data reader"));

        if (i >= this.FieldCount)
            this.Throw (new IndexOutOfRangeException ());

        return this.ResultSet.Fields [i].ColumnName;
    }

    /// <summary>
    /// Gets the column ordinal, given the name of the column.
    /// </summary>
    /// <param name="name">The name of the column.</param>
    /// <returns>The zero-based column ordinal.</returns>
    public override int GetOrdinal (string name)
    {
        if (!this._isOpen || this.ResultSet == null)
            this.Throw (new Exception ("No current query in data reader"));

        return this.ResultSet.GetOrdinal (name);
    }

    /// <summary>
    /// Gets a stream to retrieve data from the specified column.
    /// </summary>
    /// <param name="ordinal">The zero-based column ordinal.</param>
    /// <returns>A stream</returns>
    public override Stream GetStream (int i)
    {
        if (i >= this.FieldCount)
            this.Throw (new IndexOutOfRangeException ());

        IMySqlValue val = this.GetFieldValue (i, false);

        if (!(val is MySqlBinary) && !(val is MySqlGuid))
            this.Throw (new MySqlException ("GetStream can only be called on binary or guid columns"));

        byte [] bytes = new byte[0];

        if (val is MySqlBinary)
            bytes = ((MySqlBinary) val).Value;
        else
            bytes = ((MySqlGuid) val).Bytes;

        return new MemoryStream (bytes, false);
    }

    /// <summary>
    ///  Gets the value of the specified column as a <see cref="String"/> object.
    /// </summary>
    /// <remarks>
    ///  <para>No conversions are performed; therefore, the data retrieved must already be a <see cref="String"/> object.</para>
    ///  <para>Call <see cref="IsDBNull"/> to check for null values before calling this method.</para>
    /// </remarks>
    /// <param name="column">The column name.</param>
    /// <returns>The value of the specified column.</returns>
    public string GetString (string column)
    {
        return this.GetString (this.GetOrdinal (column));
    }

    /// <summary>
    ///  Gets the value of the specified column as a <see cref="String"/> object.
    /// </summary>
    /// <remarks>
    ///  <para>No conversions are performed; therefore, the data retrieved must already be a <see cref="String"/> object.</para>
    ///  <para>Call <see cref="IsDBNull"/> to check for null values before calling this method.</para>
    /// </remarks>
    /// <param name="i">The zero-based column ordinal.</param>
    /// <returns>The value of the specified column.</returns>
    public override string GetString (int i)
    {
        IMySqlValue val = this.GetFieldValue (i, true);

        if (val is MySqlBinary)
        {
            byte [] v = ((MySqlBinary) val).Value;
            return this.ResultSet.Fields [i].Encoding.GetString (v, 0, v.Length);
        }

        return val.Value.ToString ();
    }

    /// <summary>
    ///  Gets the value of the specified column as a <see cref="TimeSpan"/> object.
    /// </summary>
    /// <remarks>
    ///  <para> No conversions are performed; therefore, the data retrieved must already be a <see cref="TimeSpan"/> value.</para>
    ///  <para>Call <see cref="IsDBNull"/> to check for null values before calling this method.</para>
    /// </remarks>
    /// <param name="column">The column name.</param>
    /// <returns>The value of the specified column.</returns>
    public TimeSpan GetTimeSpan (string column)
    {
        return this.GetTimeSpan (this.GetOrdinal (column));
    }

    /// <summary>
    ///  Gets the value of the specified column as a <see cref="TimeSpan"/> object.
    /// </summary>
    /// <remarks>
    ///  <para>No conversions are performed; therefore, the data retrieved must already be a <see cref="TimeSpan"/> value.</para>
    ///  <para>Call <see cref="IsDBNull"/> to check for null values before calling this method.</para>
    /// </remarks>
    /// <param name="column">The zero-based column ordinal.</param>
    /// <returns>The value of the specified column.</returns>
    public TimeSpan GetTimeSpan (int column)
    {
        IMySqlValue val = this.GetFieldValue (column, true);

        MySqlTimeSpan ts = (MySqlTimeSpan) val;
        return ts.Value;
    }

    /// <summary>
    /// Gets the value of the specified column in its native format.
    /// </summary>
    /// <param name="i">The zero-based column ordinal.</param>
    /// <returns>The value of the specified column.</returns>
    public override object GetValue (int i)
    {
        if (!this._isOpen)
            this.Throw (new Exception ("No current query in data reader"));

        if (i >= this.FieldCount)
            this.Throw (new IndexOutOfRangeException ());

        IMySqlValue val = this.GetFieldValue (i, false);

        if (val.IsNull)
            if (!(val.MySqlDbType == MySqlDbType.Time && val.Value.ToString () == "00:00:00"))
                return DBNull.Value;

        // if the column is a date/time, then we return a MySqlDateTime
        // so .ToString() will print '0000-00-00' correctly
        if (val is MySqlDateTime)
        {
            MySqlDateTime dt = (MySqlDateTime) val;

            if (!dt.IsValidDateTime && this._connection.Settings.ConvertZeroDateTime)
                return DateTime.MinValue;
            else if (this._connection.Settings.AllowZeroDateTime)
                return val;
            else
                return dt.GetDateTime ();
        }

        return val.Value;
    }

    /// <summary>
    /// Gets all attribute columns in the collection for the current row.
    /// </summary>
    /// <param name="values">An array of <see cref="Object"/> into which to copy the attribute columns.</param>
    /// <returns>The number of instances of <see cref="Object"/> in the array.</returns>
    public override int GetValues (object [] values)
    {
        int numCols = Math.Min (values.Length, this.FieldCount);

        for (int i = 0; i < numCols; i++)
            values [i] = this.GetValue (i);

        return numCols;
    }

    /// <summary>Gets the value of the specified column as a 16-bit unsigned integer.</summary>
    /// <remarks>
    ///  <para>No conversions are performed; therefore, the data retrieved must already be a <see cref="UInt16"/> value.</para>
    ///  <para>Call <see cref="IsDBNull"/> to check for null values before calling this method.</para>
    /// </remarks>
    /// <param name="column">The column name.</param>
    /// <returns>The value of the specified column.</returns>
    public ushort GetUInt16 (string column)
    {
        return this.GetUInt16 (this.GetOrdinal (column));
    }

    /// <summary>Gets the value of the specified column as a 16-bit unsigned integer.</summary>
    /// <remarks>
    ///  <para>No conversions are performed; therefore, the data retrieved must already be a <see cref="UInt16"/> value.</para>
    ///  <para>Call <see cref="IsDBNull"/> to check for null values before calling this method.</para>
    /// </remarks>
    /// <param name="column">The zero-based column ordinal.</param>
    /// <returns>The value of the specified column.</returns>
    public ushort GetUInt16 (int column)
    {
        IMySqlValue v = this.GetFieldValue (column, true);

        if (v is MySqlUInt16)
            return ((MySqlUInt16) v).Value;

        return (ushort) this.ChangeType (v, column, typeof (ushort));
    }

    /// <summary>Gets the value of the specified column as a 32-bit unsigned integer.</summary>
    /// <remarks>
    ///  <para>No conversions are performed; therefore, the data retrieved must already be a <see cref="UInt32"/> value.</para>
    ///  <para>Call <see cref="IsDBNull"/> to check for null values before calling this method.</para>
    /// </remarks>
    /// <param name="column">The column name.</param>
    /// <returns>The value of the specified column.</returns>
    public uint GetUInt32 (string column)
    {
        return this.GetUInt32 (this.GetOrdinal (column));
    }

    /// <summary>Gets the value of the specified column as a 32-bit unsigned integer.</summary>
    /// <remarks>
    ///  <para>No conversions are performed; therefore, the data retrieved must already be a <see cref="UInt32"/> value.</para>
    ///  <para>Call <see cref="IsDBNull"/> to check for null values before calling this method.</para>
    /// </remarks>
    /// <param name="column">The zero-based column ordinal.</param>
    /// <returns>The value of the specified column.</returns>
    public uint GetUInt32 (int column)
    {
        IMySqlValue v = this.GetFieldValue (column, true);

        if (v is MySqlUInt32)
            return ((MySqlUInt32) v).Value;

        return (uint) this.ChangeType (v, column, typeof (uint));
    }

    /// <summary>Gets the value of the specified column as a 64-bit unsigned integer.</summary>
    /// <remarks>
    ///  <para>No conversions are performed; therefore, the data retrieved must already be a <see cref="UInt64"/> value.</para>
    ///  <para>Call <see cref="IsDBNull"/> to check for null values before calling this method.</para>
    /// </remarks>
    /// <param name="column">The column name.</param>
    /// <returns>The value of the specified column.</returns>
    public ulong GetUInt64 (string column)
    {
        return this.GetUInt64 (this.GetOrdinal (column));
    }

    /// <summary>Gets the value of the specified column as a 64-bit unsigned integer.</summary>
    /// <remarks>
    ///  <para>No conversions are performed; therefore, the data retrieved must already be a <see cref="UInt64"/> value.</para>
    ///  <para>Call <see cref="IsDBNull"/> to check for null values before calling this method.</para>
    /// </remarks>
    /// <param name="column">The zero-based column ordinal.</param>
    /// <returns>The value of the specified column.</returns>
    public ulong GetUInt64 (int column)
    {
        IMySqlValue v = this.GetFieldValue (column, true);

        if (v is MySqlUInt64)
            return ((MySqlUInt64) v).Value;

        return (ulong) this.ChangeType (v, column, typeof (ulong));
    }

#endregion

    /// <summary>
    /// Returns a <see cref="DbDataReader"/> object for the requested column ordinal.
    /// </summary>
    /// <param name="i">The zero-based column ordinal.</param>
    /// <returns>A <see cref="DbDataReader"/> object.</returns>
    IDataReader IDataRecord.GetData (int i)
    {
        return base.GetData (i);
    }

    /// <summary>
    /// Gets a value indicating whether the column contains non-existent or missing values.
    /// </summary>
    /// <param name="i">The zero-based column ordinal.</param>
    /// <returns>true if the specified column is equivalent to <see cref="DBNull"/>; otherwise false.</returns>
    public override bool IsDBNull (int i)
    {
        return DBNull.Value == this.GetValue (i);
    }

    /// <summary>
    /// Advances the data reader to the next result, when reading the results of batch SQL statements.
    /// </summary>
    /// <returns>true if there are more result sets; otherwise false.</returns>
    public override bool NextResult ()
    {
        if (!this._isOpen)
            this.Throw (new MySqlException (Resources.NextResultIsClosed));

        bool isCaching = this.Command.CommandType == CommandType.TableDirect && this.Command.EnableCaching &&
                         (this.CommandBehavior & CommandBehavior.SequentialAccess) == 0;

        // this will clear out any unread data
        if (this.ResultSet != null)
        {
            this.ResultSet.Close ();

            if (isCaching)
                TableCache.AddToCache (this.Command.CommandText, this.ResultSet);
        }

        // single result means we only return a single resultset.  If we have already
        // returned one, then we return false
        // TableDirect is basically a select * from a single table so it will generate
        // a single result also
        if (this.ResultSet != null &&
            ((this.CommandBehavior & CommandBehavior.SingleResult) != 0 || isCaching))
            return false;

        // next load up the next resultset if any
        try
        {
            do
            {
                this.ResultSet = null;

                // if we are table caching, then try to retrieve the resultSet from the cache
                if (isCaching)
                    this.ResultSet = TableCache.RetrieveFromCache (this.Command.CommandText, this.Command.CacheAge);

                if (this.ResultSet == null)
                {
                    this.ResultSet = this.driver.NextResult (this.Statement.StatementId, false);

                    if (this.ResultSet == null)
                        return false;

                    if (this.ResultSet.IsOutputParameters && this.Command.CommandType == CommandType.StoredProcedure)
                    {
                        StoredProcedure sp = this.Statement as StoredProcedure;
                        sp.ProcessOutputParameters (this);
                        this.ResultSet.Close ();

                        for (int i = 0; i < this.ResultSet.Fields.Length; i++)
                            if (this.ResultSet.Fields [i].ColumnName.StartsWith ("@" + StoredProcedure.ParameterPrefix, StringComparison.OrdinalIgnoreCase))
                            {
                                this.ResultSet = null;
                                break;
                            }

                        if (!sp.ServerProvidingOutputParameters)
                            return false;

                        // if we are using server side output parameters then we will get our ok packet
                        // *after* the output parameters resultset
                        this.ResultSet = this.driver.NextResult (this.Statement.StatementId, true);
                    }

                    this.ResultSet.Cached = isCaching;
                }

                if (this.ResultSet.Size == 0)
                {
                    this.Command.LastInsertedId = this.ResultSet.InsertedId;

                    if (this.affectedRows == -1)
                        this.affectedRows = this.ResultSet.AffectedRows;
                    else
                        this.affectedRows += this.ResultSet.AffectedRows;
                }
            }
            while (this.ResultSet.Size == 0);

            return true;
        }
        catch (MySqlException ex)
        {
            if (ex.IsFatal)
                this._connection.Abort ();

            if (ex.Number == 0)
                throw new MySqlException (Resources.FatalErrorReadingResult, ex);

            if ((this.CommandBehavior & CommandBehavior.CloseConnection) != 0)
                this.Close ();

            throw;
        }
    }

    /// <summary>
    /// Advances the <see cref="MySqlDataReader"/> to the next record.
    /// </summary>
    /// <returns>true if there are more rows; otherwise false.</returns>
    public override bool Read ()
    {
        if (!this._isOpen)
            this.Throw (new MySqlException ("Invalid attempt to Read when reader is closed."));

        if (this.ResultSet == null)
            return false;

        try
        {
            return this.ResultSet.NextRow (this.CommandBehavior);
        }
        catch (TimeoutException tex)
        {
            this._connection.HandleTimeoutOrThreadAbort (tex);
            throw; // unreached
        }
        catch (ThreadAbortException taex)
        {
            this._connection.HandleTimeoutOrThreadAbort (taex);
            throw;
        }
        catch (MySqlException ex)
        {
            if (ex.IsFatal)
                this._connection.Abort ();

            if (ex.IsQueryAborted)
                throw;

            throw new MySqlException (Resources.FatalErrorDuringRead, ex);
        }
    }

    /// <summary>
    /// Gets the value of the specified column as a <see cref="MySqlGeometry"/>.
    /// </summary>
    /// <param name="i">The index of the colum.</param>
    /// <returns>The value of the specified column as a <see cref="MySqlGeometry"/>.</returns>
    public MySqlGeometry GetMySqlGeometry (int i)
    {
        try
        {
            IMySqlValue v = this.GetFieldValue (i, false);

            if (v is MySqlGeometry || v is MySqlBinary)
                return new MySqlGeometry (MySqlDbType.Geometry, (byte []) v.Value);
        }
        catch
        {
            this.Throw (new Exception ("Can't get MySqlGeometry from value"));
        }

        return new MySqlGeometry (true);
    }

    /// <summary>
    /// Gets the value of the specified column as a <see cref="MySqlGeometry"/>.
    /// </summary>
    /// <param name="column">The name of the colum.</param>
    /// <returns>The value of the specified column as a <see cref="MySqlGeometry"/>.</returns>
    public MySqlGeometry GetMySqlGeometry (string column)
    {
        return this.GetMySqlGeometry (this.GetOrdinal (column));
    }

    /// <summary>
    /// Returns an <see cref="IEnumerator"/> that iterates through the <see cref="MySqlDataReader"/>. 
    /// </summary>
    /// <returns>An <see cref="IEnumerator"/> that can be used to iterate through the rows in the data reader.</returns>
    public override IEnumerator GetEnumerator ()
    {
        return new DbEnumerator (this, (this.CommandBehavior & CommandBehavior.CloseConnection) != 0);
    }

    private IMySqlValue GetFieldValue (int index, bool checkNull)
    {
        if (index < 0 || index >= this.FieldCount)
            this.Throw (new ArgumentException (Resources.InvalidColumnOrdinal));

        IMySqlValue v = this.ResultSet [index];

        if (!(v.MySqlDbType is MySqlDbType.Time && v.Value.ToString () == "00:00:00"))
            if (checkNull && v.IsNull)
                throw new System.Data.SqlTypes.SqlNullValueException ();

        return v;
    }

    /// <summary>
    /// Returns the field's encoding
    /// </summary>
    /// <param name="index"></param>
    /// <returns></returns>
    public Encoding GetEncoding (int index)
    {
        if (index < 0 || index >= this.FieldCount)
            this.Throw (new ArgumentException (Resources.InvalidColumnOrdinal));

        return this.ResultSet.Fields [index].Encoding;
    }

    private void ClearKillFlag ()
    {
        // This query will silently crash because of the Kill call that happened before.
        string       dummyStatement = "SELECT * FROM bogus_table LIMIT 0"; /* dummy query used to clear kill flag */
        MySqlCommand dummyCommand   = new MySqlCommand (dummyStatement, this._connection) {InternallyCreated = true};

        try
        {
            dummyCommand.ExecuteReader (); // ExecuteReader catches the exception and returns null, which is expected.
        }
        catch (MySqlException ex)
        {
            int [] errors = {(int) MySqlErrorCode.NoSuchTable, (int) MySqlErrorCode.TableAccessDenied, (int) MySqlErrorCode.UnknownTable};

            if (Array.IndexOf (errors, (int) ex.Number) < 0)
                throw;
        }
    }

    /// <summary>
    /// Gets the value of the specified column as a type.
    /// </summary>
    /// <typeparam name="T">Type.</typeparam>
    /// <param name="ordinal">The index of the column.</param>
    /// <returns>The value of the column.</returns>
    public override T GetFieldValue <T> (int ordinal)
    {
        if (typeof (T).Equals (typeof (DateTimeOffset)))
        {
            DateTime dtValue  = new DateTime ();
            bool     result   = DateTime.TryParse (this.GetValue (ordinal).ToString (), out dtValue);
            DateTime datetime = result ? dtValue : DateTime.MinValue;
            return (T) Convert.ChangeType (new DateTimeOffset (datetime), typeof (T));
        }
        else if (typeof (T).Equals (typeof (Stream)))
        {
            return (T) (object) this.GetStream (ordinal);
        }
        else
        {
            return base.GetFieldValue <T> (ordinal);
        }
    }

    private void Throw (Exception ex)
    {
        this._connection?.Throw (ex);
        throw ex;
    }

    /// <summary>
    /// Releases all resources used by the current instance of the <see cref="MySqlDataReader"/> class.
    /// </summary>
    public new void Dispose ()
    {
        this.Dispose (true);
        GC.SuppressFinalize (this);
    }

    internal new void Dispose (bool disposing)
    {
        if (disposing)
            this.Close ();
    }

#region Destructor

    ~MySqlDataReader ()
    {
        this.Dispose (false);
    }

#endregion
}
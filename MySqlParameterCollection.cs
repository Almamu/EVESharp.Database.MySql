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

using System;
using System.Data.Common;
using System.Collections.Generic;
using System.Collections;
using EVESharp.Database.MySql;

namespace EVESharp.Database.MySql;

/// <summary>
/// Represents a collection of parameters relevant to a <see cref="MySqlCommand"/> 
/// as well as their respective mappings to columns in a <see cref="System.Data.DataSet"/>. This class cannot be inherited.
/// </summary>
/// <remarks>
///  The number of the parameters in the collection must be equal to the number of
///  parameter placeholders within the command text, or an exception will be generated.
///</remarks>
public sealed partial class MySqlParameterCollection : DbParameterCollection
{
    private readonly List <MySqlParameter>    _items = new List <MySqlParameter> ();
    private readonly Dictionary <string, int> _indexHashCs;
    private readonly Dictionary <string, int> _indexHashCi;
    //turns to true if any parameter is unnamed
    internal bool containsUnnamedParameters;

    internal MySqlParameterCollection (MySqlCommand cmd)
    {
        this._indexHashCs              = new Dictionary <string, int> ();
        this._indexHashCi              = new Dictionary <string, int> (StringComparer.CurrentCultureIgnoreCase);
        this.containsUnnamedParameters = false;
        this.Clear ();
    }

    /// <summary>
    /// Gets the number of MySqlParameter objects in the collection.
    /// </summary>
    public override int Count => this._items.Count;

#region Public Methods

    /// <summary>
    /// Gets the <see cref="MySqlParameter"/> at the specified index.
    /// </summary>
    /// <overloads>Gets the <see cref="MySqlParameter"/> with a specified attribute.
    /// [C#] In C#, this property is the indexer for the <see cref="MySqlParameterCollection"/> class.
    /// </overloads>
    public new MySqlParameter this [int index]
    {
        get => this.InternalGetParameter (index);
        set => this.InternalSetParameter (index, value);
    }

    /// <summary>
    /// Gets the <see cref="MySqlParameter"/> with the specified name.
    /// </summary>
    public new MySqlParameter this [string name]
    {
        get => this.InternalGetParameter (name);
        set => this.InternalSetParameter (name, value);
    }

    /// <summary>
    /// Adds a <see cref="MySqlParameter"/> to the <see cref="MySqlParameterCollection"/> with the parameter name, the data type, the column length, and the source column name.
    /// </summary>
    /// <param name="parameterName">The name of the parameter.</param>
    /// <param name="dbType">One of the <see cref="MySqlDbType"/> values. </param>
    /// <param name="size">The length of the column.</param>
    /// <param name="sourceColumn">The name of the source column.</param>
    /// <returns>The newly added <see cref="MySqlParameter"/> object.</returns>
    public MySqlParameter Add (string parameterName, MySqlDbType dbType, int size, string sourceColumn)
    {
        return this.Add (new MySqlParameter (parameterName, dbType, size, sourceColumn));
    }

    /// <summary>
    /// Adds the specified <see cref="MySqlParameter"/> object to the <see cref="MySqlParameterCollection"/>.
    /// </summary>
    /// <param name="value">The <see cref="MySqlParameter"/> to add to the collection.</param>
    /// <returns>The newly added <see cref="MySqlParameter"/> object.</returns>
    public MySqlParameter Add (MySqlParameter value)
    {
        return this.InternalAdd (value, null);
    }

    /// <summary>
    /// Adds a parameter and its value.
    /// </summary>
    /// <param name="parameterName">The name of the parameter.</param>
    /// <param name="value">The value of the parameter.</param>
    /// <returns>A <see cref="MySqlParameter"/> object representing the provided values.</returns>
    public MySqlParameter AddWithValue (string parameterName, object value)
    {
        return this.Add (new MySqlParameter (parameterName, value));
    }

    /// <summary>
    /// Adds a <see cref="MySqlParameter"/> to the <see cref="MySqlParameterCollection"/> given the parameter name and the data type.
    /// </summary>
    /// <param name="parameterName">The name of the parameter.</param>
    /// <param name="dbType">One of the <see cref="MySqlDbType"/> values. </param>
    /// <returns>The newly added <see cref="MySqlParameter"/> object.</returns>
    public MySqlParameter Add (string parameterName, MySqlDbType dbType)
    {
        return this.Add (new MySqlParameter (parameterName, dbType));
    }

    /// <summary>
    /// Adds a <see cref="MySqlParameter"/> to the <see cref="MySqlParameterCollection"/> with the parameter name, the data type, and the column length.
    /// </summary>
    /// <param name="parameterName">The name of the parameter.</param>
    /// <param name="dbType">One of the <see cref="MySqlDbType"/> values. </param>
    /// <param name="size">The length of the column.</param>
    /// <returns>The newly added <see cref="MySqlParameter"/> object.</returns>
    public MySqlParameter Add (string parameterName, MySqlDbType dbType, int size)
    {
        return this.Add (new MySqlParameter (parameterName, dbType, size));
    }

#endregion

    /// <summary>
    /// Removes all items from the collection.
    /// </summary>
    public override void Clear ()
    {
        foreach (MySqlParameter p in this._items)
            p.Collection = null;

        this._items.Clear ();
        this._indexHashCs.Clear ();
        this._indexHashCi.Clear ();
    }

    private void CheckIndex (int index)
    {
        if (index < 0 || index >= this.Count)
            throw new IndexOutOfRangeException ("Parameter index is out of range.");
    }

    private MySqlParameter InternalGetParameter (int index)
    {
        this.CheckIndex (index);
        return this._items [index];
    }

    private MySqlParameter InternalGetParameter (string parameterName)
    {
        int index = this.IndexOf (parameterName);

        if (index < 0)
        {
            // check to see if the user has added the parameter without a
            // parameter marker.  If so, kindly tell them what they did.
            if (parameterName.StartsWith ("@", StringComparison.Ordinal) ||
                parameterName.StartsWith ("?", StringComparison.Ordinal))
            {
                string newParameterName = parameterName.Substring (1);
                index = this.IndexOf (newParameterName);

                if (index != -1)
                    return this._items [index];
            }

            throw new ArgumentException ("Parameter '" + parameterName + "' not found in the collection.");
        }

        return this._items [index];
    }

    private void InternalSetParameter (string parameterName, MySqlParameter value)
    {
        int index = this.IndexOf (parameterName);

        if (index < 0)
            throw new ArgumentException ("Parameter '" + parameterName + "' not found in the collection.");

        this.InternalSetParameter (index, value);
    }

    private void InternalSetParameter (int index, MySqlParameter value)
    {
        MySqlParameter newParameter = value;

        if (newParameter == null)
            throw new ArgumentException (Resources.NewValueShouldBeMySqlParameter);

        this.CheckIndex (index);
        MySqlParameter p = this._items [index];

        // first we remove the old parameter from our hashes
        this._indexHashCs.Remove (p.ParameterName);
        this._indexHashCi.Remove (p.ParameterName);

        // then we add in the new parameter
        this._items [index] = newParameter;
        this._indexHashCs.Add (value.ParameterName, index);
        this._indexHashCi.Add (value.ParameterName, index);
    }

    /// <summary>
    /// Gets the location of the <see cref="MySqlParameter"/> in the collection with a specific parameter name.
    /// </summary>
    /// <param name="parameterName">The name of the <see cref="MySqlParameter"/> object to retrieve. </param>
    /// <returns>The zero-based location of the <see cref="MySqlParameter"/> in the collection.</returns>
    public override int IndexOf (string parameterName)
    {
        int i = -1;

        if (!this._indexHashCs.TryGetValue (parameterName, out i) &&
            !this._indexHashCi.TryGetValue (parameterName, out i))
            return -1;

        return i;
    }

    /// <summary>
    /// Gets the location of a <see cref="MySqlParameter"/> in the collection.
    /// </summary>
    /// <param name="value">The <see cref="MySqlParameter"/> object to locate. </param>
    /// <returns>The zero-based location of the <see cref="MySqlParameter"/> in the collection.</returns>
    /// <overloads>Gets the location of a <see cref="MySqlParameter"/> in the collection.</overloads>
    public override int IndexOf (object value)
    {
        MySqlParameter parameter = value as MySqlParameter;

        if (null == parameter)
            throw new ArgumentException ("Argument must be of type DbParameter", "value");

        return this._items.IndexOf (parameter);
    }

    internal void ParameterNameChanged (MySqlParameter p, string oldName, string newName)
    {
        int index = this.IndexOf (oldName);
        this._indexHashCs.Remove (oldName);
        this._indexHashCi.Remove (oldName);

        this._indexHashCs.Add (newName, index);
        this._indexHashCi.Add (newName, index);
    }

    private MySqlParameter InternalAdd (MySqlParameter value, int? index)
    {
        if (value == null)
            throw new ArgumentException ("The MySqlParameterCollection only accepts non-null MySqlParameter type objects.", "value");

        // if the parameter is unnamed, then assign a default name
        if (string.IsNullOrEmpty (value.ParameterName))
            value.ParameterName = string.Format ("Parameter{0}", this.GetNextIndex ());

        // make sure we don't already have a parameter with this name
        if (this.IndexOf (value.ParameterName) >= 0)
        {
            throw new MySqlException (string.Format (Resources.ParameterAlreadyDefined, value.ParameterName));
        }
        else
        {
            string inComingName = value.ParameterName;

            if (inComingName [0] == '@' || inComingName [0] == '?')
                inComingName = inComingName.Substring (1, inComingName.Length - 1);

            if (this.IndexOf (inComingName) >= 0)
                throw new MySqlException (string.Format (Resources.ParameterAlreadyDefined, value.ParameterName));
        }

        if (index == null)
        {
            this._items.Add (value);
            index = this._items.Count - 1;
        }
        else
        {
            this._items.Insert ((int) index, value);
            this.AdjustHashes ((int) index, true);
        }

        this._indexHashCs.Add (value.ParameterName, (int) index);
        this._indexHashCi.Add (value.ParameterName, (int) index);

        value.Collection = this;
        return value;
    }

    private int GetNextIndex ()
    {
        int index = this.Count + 1;

        while (true)
        {
            string name = "Parameter" + index.ToString ();

            if (!this._indexHashCi.ContainsKey (name))
                break;

            index++;
        }

        return index;
    }

    private static void AdjustHash (Dictionary <string, int> hash, string parameterName, int keyIndex, bool addEntry)
    {
        if (!hash.ContainsKey (parameterName))
            return;

        int index = hash [parameterName];

        if (index < keyIndex)
            return;

        hash [parameterName] = addEntry ? ++index : --index;
    }

    /// <summary>
    /// This method will update all the items in the index hashes when
    /// we insert a parameter somewhere in the middle
    /// </summary>
    /// <param name="keyIndex"></param>
    /// <param name="addEntry"></param>
    private void AdjustHashes (int keyIndex, bool addEntry)
    {
        for (int i = 0; i < this.Count; i++)
        {
            string name = this._items [i].ParameterName;
            AdjustHash (this._indexHashCi, name, keyIndex, addEntry);
            AdjustHash (this._indexHashCs, name, keyIndex, addEntry);
        }
    }

    private MySqlParameter GetParameterFlexibleInternal (string baseName)
    {
        int index = this.IndexOf (baseName);

        if (-1 == index)
            index = this.IndexOf ("?" + baseName);

        if (-1 == index)
            index = this.IndexOf ("@" + baseName);

        if (-1 != index)
            return this [index];

        return null;
    }

    internal MySqlParameter GetParameterFlexible (string parameterName, bool throwOnNotFound)
    {
        string         baseName = parameterName;
        MySqlParameter p        = this.GetParameterFlexibleInternal (baseName);

        if (p != null)
            return p;

        if (parameterName.StartsWith ("@", StringComparison.Ordinal) || parameterName.StartsWith ("?", StringComparison.Ordinal))
            baseName = parameterName.Substring (1);

        p = this.GetParameterFlexibleInternal (baseName);

        if (p != null)
            return p;

        if (throwOnNotFound)
            throw new ArgumentException ("Parameter '" + parameterName + "' not found in the collection.");

        return null;
    }

#region DbParameterCollection Implementation

    /// <summary>
    /// Adds an array of values to the end of the <see cref="MySqlParameterCollection"/>. 
    /// </summary>
    /// <param name="values"></param>
    public override void AddRange (Array values)
    {
        foreach (DbParameter p in values)
            this.Add (p);
    }

    /// <summary>
    /// Retrieve the parameter with the given name.
    /// </summary>
    /// <param name="parameterName"></param>
    /// <returns></returns>
    protected override DbParameter GetParameter (string parameterName)
    {
        return this.InternalGetParameter (parameterName);
    }

    protected override DbParameter GetParameter (int index)
    {
        return this.InternalGetParameter (index);
    }

    protected override void SetParameter (string parameterName, DbParameter value)
    {
        this.InternalSetParameter (parameterName, value as MySqlParameter);
    }

    protected override void SetParameter (int index, DbParameter value)
    {
        this.InternalSetParameter (index, value as MySqlParameter);
    }

    /// <summary>
    /// Adds the specified <see cref="MySqlParameter"/> object to the <see cref="MySqlParameterCollection"/>.
    /// </summary>
    /// <param name="value">The <see cref="MySqlParameter"/> to add to the collection.</param>
    /// <returns>The index of the new <see cref="MySqlParameter"/> object.</returns>
    public override int Add (object value)
    {
        MySqlParameter parameter = value as MySqlParameter;

        if (parameter == null)
            throw new MySqlException ("Only MySqlParameter objects may be stored");

        parameter = this.Add (parameter);
        return this.IndexOf (parameter);
    }

    /// <summary>
    /// Gets a value indicating whether a <see cref="MySqlParameter"/> with the specified parameter name exists in the collection.
    /// </summary>
    /// <param name="parameterName">The name of the <see cref="MySqlParameter"/> object to find.</param>
    /// <returns>true if the collection contains the parameter; otherwise, false.</returns>
    public override bool Contains (string parameterName)
    {
        return this.IndexOf (parameterName) != -1;
    }

    /// <summary>
    /// Gets a value indicating whether a MySqlParameter exists in the collection.
    /// </summary>
    /// <param name="value">The value of the <see cref="MySqlParameter"/> object to find. </param>
    /// <returns>true if the collection contains the <see cref="MySqlParameter"/> object; otherwise, false.</returns>
    /// <overloads>Gets a value indicating whether a <see cref="MySqlParameter"/> exists in the collection.</overloads>
    public override bool Contains (object value)
    {
        MySqlParameter parameter = value as MySqlParameter;

        if (null == parameter)
            throw new ArgumentException ("Argument must be of type DbParameter", nameof (value));

        return this._items.Contains (parameter);
    }

    /// <summary>
    /// Copies MySqlParameter objects from the MySqlParameterCollection to the specified array.
    /// </summary>
    /// <param name="array"></param>
    /// <param name="index"></param>
    public override void CopyTo (Array array, int index)
    {
        this._items.ToArray ().CopyTo (array, index);
    }

    /// <summary>
    /// Returns an enumerator that iterates through the <see cref="MySqlParameterCollection"/>. 
    /// </summary>
    /// <returns></returns>
    public override IEnumerator GetEnumerator ()
    {
        return this._items.GetEnumerator ();
    }

    /// <summary>
    /// Inserts a MySqlParameter into the collection at the specified index.
    /// </summary>
    /// <param name="index"></param>
    /// <param name="value"></param>
    public override void Insert (int index, object value)
    {
        MySqlParameter parameter = value as MySqlParameter;

        if (parameter == null)
            throw new MySqlException ("Only MySqlParameter objects may be stored");

        this.InternalAdd (parameter, index);
    }

    /// <summary>
    /// Removes the specified MySqlParameter from the collection.
    /// </summary>
    /// <param name="value"></param>
    public override void Remove (object value)
    {
        MySqlParameter p = value as MySqlParameter;
        p.Collection = null;
        int index = this.IndexOf (p);
        this._items.Remove (p);

        this._indexHashCs.Remove (p.ParameterName);
        this._indexHashCi.Remove (p.ParameterName);
        this.AdjustHashes (index, false);
    }

    /// <summary>
    /// Removes the specified <see cref="MySqlParameter"/> from the collection using the parameter name.
    /// </summary>
    /// <param name="parameterName">The name of the <see cref="MySqlParameter"/> object to retrieve. </param>
    public override void RemoveAt (string parameterName)
    {
        DbParameter p = this.GetParameter (parameterName);
        this.Remove (p);
    }

    /// <summary>
    /// Removes the specified <see cref="MySqlParameter"/> from the collection using a specific index.
    /// </summary>
    /// <param name="index">The zero-based index of the parameter. </param>
    /// <overloads>Removes the specified <see cref="MySqlParameter"/> from the collection.</overloads>
    public override void RemoveAt (int index)
    {
        object o = this._items [index];
        this.Remove (o);
    }

    /// <summary>
    /// Gets an object that can be used to synchronize access to the 
    /// <see cref="MySqlParameterCollection"/>. 
    /// </summary>
    public override object SyncRoot => (this._items as IList).SyncRoot;

#endregion
}
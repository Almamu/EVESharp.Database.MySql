﻿// Copyright © 2015, 2017 Oracle and/or its affiliates. All rights reserved.
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
using Mysqlx.Expr;
using Mysqlx.Datatypes;
using Mysqlx.Crud;
using Google.Protobuf;
using System.Collections.Generic;

namespace MySqlX.Protocol.X
{
  internal class ExprUtil
  {
    /**
     * Proto-buf helper to build a LITERAL Expr with a Scalar NULL type.
     */
    public static Expr BuildLiteralNullScalar()
    {
      return BuildLiteralExpr(NullScalar());
    }

    /**
     * Proto-buf helper to build a LITERAL Expr with a Scalar DOUBLE type (wrapped in Any).
     */
    public static Expr BuildLiteralScalar(double d)
    {
      return BuildLiteralExpr(ScalarOf(d));
    }

    /**
     * Proto-buf helper to build a LITERAL Expr with a Scalar SINT (signed int) type (wrapped in Any).
     */
    public static Expr BuildLiteralScalar(long l)
    {
      return BuildLiteralExpr(ScalarOf(l));
    }

    /**
     * Proto-buf helper to build a LITERAL Expr with a Scalar STRING type (wrapped in Any).
     */
    public static Expr BuildLiteralScalar(String str)
    {
      return BuildLiteralExpr(ScalarOf(str));
    }

    /**
     * Proto-buf helper to build a LITERAL Expr with a Scalar OCTETS type (wrapped in Any).
     */
    public static Expr BuildLiteralScalar(byte[] bytes)
    {
      return BuildLiteralExpr(ScalarOf(bytes));
    }

    /**
     * Proto-buf helper to build a LITERAL Expr with a Scalar BOOL type (wrapped in Any).
     */
    public static Expr BuildLiteralScalar(Boolean b)
    {
      return BuildLiteralExpr(ScalarOf(b));
    }

    /**
     * Wrap an Any value in a LITERAL expression.
     */
    public static Expr BuildLiteralExpr(Scalar scalar)
    {
      return new Expr() { Type = Expr.Types.Type.Literal, Literal = scalar };
    }

    public static Scalar NullScalar()
    {
      return new Scalar() { Type = Scalar.Types.Type.VNull };
    }

    public static Scalar ScalarOf(double d)
    {
      return new Scalar() { Type = Scalar.Types.Type.VDouble, VDouble = d };
    }

    public static Scalar ScalarOf(long l)
    {
      return new Scalar() { Type = Scalar.Types.Type.VSint, VSignedInt = l};
    }

    public static Scalar ScalarOf(String str)
    {
      Scalar.Types.String strValue = new Scalar.Types.String() { Value = ByteString.CopyFromUtf8(str) };
      return new Scalar() { Type = Scalar.Types.Type.VString, VString = strValue };
    }

    public static Scalar ScalarOf(byte[] bytes)
    {
      return new Scalar() { Type = Scalar.Types.Type.VOctets, VOctets = new Scalar.Types.Octets() { Value = ByteString.CopyFrom(bytes) } };
    }

    public static Scalar ScalarOf(Boolean b)
    {
      return new Scalar() { Type = Scalar.Types.Type.VBool, VBool = b };
    }

    /**
     * Build an Any with a string value.
     */
    public static Any BuildAny(String str)
    {
      // same as Expr
      Scalar.Types.String sstr = new Scalar.Types.String();
      sstr.Value = ByteString.CopyFromUtf8(str);
      Scalar s = new Scalar();
      s.Type = Scalar.Types.Type.VString;
      s.VString = sstr;
      Any a = new Any();
      a.Type = Any.Types.Type.Scalar;
      a.Scalar = s;
      return a;
    }

    public static Any BuildAny(Boolean b)
    {
      return new Any() { Type = Any.Types.Type.Scalar, Scalar = ScalarOf(b) };
    }

    public static Any BuildAny(object value)
    {
      return new Any() { Type = Any.Types.Type.Scalar, Scalar = ExprUtil.ArgObjectToScalar(value) };
    }

    public static Collection BuildCollection(String schemaName, String collectionName)
    {
      return new Collection() { Schema = schemaName, Name = collectionName };
    }

    public static Scalar ArgObjectToScalar(System.Object value)
    {
      return ArgObjectToExpr(value, false).Literal;
    }

    public static Expr ArgObjectToExpr(System.Object value, Boolean allowRelationalColumns)
    {
      if (value == null)
        return BuildLiteralNullScalar();

      if (value is Dictionary<string, object>)
        value = new XDevAPI.DbDoc(value).ToString();

      if (value is bool)
        return BuildLiteralScalar(Convert.ToBoolean(value));
      else if (value is byte || value is short || value is int || value is long)
        return BuildLiteralScalar(Convert.ToInt64(value));
      else if (value is float || value is double)
        return BuildLiteralScalar(Convert.ToDouble(value));
      else if (value is string)
      {
        try
        {
          // try to parse expressions
          Expr expr = new ExprParser((string)value).Parse();
          if (expr.Identifier != null)
            return BuildLiteralScalar((string)value);
          return expr;
        }
        catch
        {
          // if can't parse, returns as literal
          return BuildLiteralScalar((string)value);
        }
      }
      else if (value is XDevAPI.DbDoc)
        return (BuildLiteralScalar(value.ToString()));
      throw new NotSupportedException("Value of type " + value.GetType() + " is not currently supported.");
    }

    public static string JoinString(string[] values)
    {
      if (values == null) return string.Empty;
      return string.Join(", ", values);
    }
  }
}

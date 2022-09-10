// Copyright (c) 2004, 2016, Oracle and/or its affiliates. All rights reserved.
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
using System.Collections;
using System.Text;

namespace EVESharp.Database.MySql.Common;

internal class ContextString
{
    private readonly bool _escapeBackslash;

    // Create a private ctor so the compiler doesn't give us a default one
    public ContextString (string contextMarkers, bool escapeBackslash)
    {
        this.ContextMarkers   = contextMarkers;
        this._escapeBackslash = escapeBackslash;
    }

    public string ContextMarkers { get; set; }

    public int IndexOf (string src, string target)
    {
        return this.IndexOf (src, target, 0);
    }

    public int IndexOf (string src, string target, int startIndex)
    {
        int index = src.IndexOf (target, startIndex);

        while (index != -1)
        {
            if (!this.IndexInQuotes (src, index, startIndex))
                break;

            index = src.IndexOf (target, index + 1);
        }

        return index;
    }

    private bool IndexInQuotes (string src, int index, int startIndex)
    {
        char contextMarker = char.MinValue;
        bool escaped       = false;

        for (int i = startIndex; i < index; i++)
        {
            char c = src [i];

            int contextIndex = this.ContextMarkers.IndexOf (c);

            // if we have found the closing marker for our open marker, then close the context
            if (contextIndex > -1 && contextMarker == this.ContextMarkers [contextIndex] && !escaped)
                contextMarker = char.MinValue;

            // if we have found a context marker and we are not in a context yet, then start one
            else if (contextMarker == char.MinValue && contextIndex > -1 && !escaped)
                contextMarker = c;

            else if (c == '\\' && this._escapeBackslash)
                escaped = !escaped;
        }

        return contextMarker != char.MinValue || escaped;
    }

    public int IndexOf (string src, char target)
    {
        char contextMarker = char.MinValue;
        bool escaped       = false;
        int  pos           = 0;

        foreach (char c in src)
        {
            int contextIndex = this.ContextMarkers.IndexOf (c);

            // if we have found the closing marker for our open marker, then close the context
            if (contextIndex > -1 && contextMarker == this.ContextMarkers [contextIndex] && !escaped)
                contextMarker = char.MinValue;

            // if we have found a context marker and we are not in a context yet, then start one
            else if (contextMarker == char.MinValue && contextIndex > -1 && !escaped)
                contextMarker = c;

            else if (contextMarker == char.MinValue && c == target)
                return pos;
            else if (c == '\\' && this._escapeBackslash)
                escaped = !escaped;

            pos++;
        }

        return -1;
    }

    public string [] Split (string src, string delimiters)
    {
        ArrayList     parts   = new ArrayList ();
        StringBuilder sb      = new StringBuilder ();
        bool          escaped = false;

        char contextMarker = char.MinValue;

        foreach (char c in src)
            if (delimiters.IndexOf (c) != -1 && !escaped)
            {
                if (contextMarker != char.MinValue)
                {
                    sb.Append (c);
                }
                else
                {
                    if (sb.Length <= 0)
                        continue;

                    parts.Add (sb.ToString ());
                    sb.Remove (0, sb.Length);
                }
            }
            else if (c == '\\' && this._escapeBackslash)
            {
                escaped = !escaped;
            }
            else
            {
                int contextIndex = this.ContextMarkers.IndexOf (c);

                if (!escaped && contextIndex != -1)
                {
                    // if we have found the closing marker for our open 
                    // marker, then close the context
                    if (contextIndex % 2 == 1)
                    {
                        if (contextMarker == this.ContextMarkers [contextIndex - 1])
                            contextMarker = char.MinValue;
                    }
                    else
                    {
                        // if the opening and closing context markers are 
                        // the same then we will always find the opening
                        // marker.
                        if (contextMarker == this.ContextMarkers [contextIndex + 1])
                            contextMarker = char.MinValue;
                        else if (contextMarker == char.MinValue)
                            contextMarker = c;
                    }
                }

                sb.Append (c);
            }

        if (sb.Length > 0)
            parts.Add (sb.ToString ());

        return (string []) parts.ToArray (typeof (string));
    }
}
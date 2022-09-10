// Copyright (c) 2004, 2021, Oracle and/or its affiliates.
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
using System.Diagnostics;
using System.IO;
using System.Text;
using EVESharp.Database.MySql;
using EVESharp.Database.MySql.Common;

namespace EVESharp.Database.MySql;

internal class MySqlPacket
{
    private          byte []      _tempBuffer = new byte[256];
    private          Encoding     _encoding;
    private readonly MemoryStream _buffer = new MemoryStream (5);

    private MySqlPacket ()
    {
        this.Clear ();
    }

    public MySqlPacket (Encoding enc)
        : this ()
    {
        this.Encoding = enc;
    }

    public MySqlPacket (MemoryStream stream)
        : this ()
    {
        this._buffer = stream;
    }

#region Properties

    public Encoding Encoding
    {
        get => this._encoding;
        set
        {
            Debug.Assert (value != null);
            this._encoding = value;
        }
    }

    public bool HasMoreData => this._buffer.Position < this._buffer.Length;

    public int Position
    {
        get => (int) this._buffer.Position;
        set => this._buffer.Position = (long) value;
    }

    public int Length
    {
        get => (int) this._buffer.Length;
        set => this._buffer.SetLength (value);
    }

    public bool IsLastPacket
    {
        get
        {
            byte [] bits = this._buffer.GetBuffer ();

            return bits [0] == 0xfe && this.Length <= 5;
        }
    }

    public byte [] Buffer
    {
        get
        {
            byte [] bits = this._buffer.GetBuffer ();

            return bits;
        }
    }

    public DBVersion Version { get; set; }

#endregion

    public void Clear ()
    {
        this.Position = 4;
    }

#region Byte methods

    public byte ReadByte ()
    {
        return (byte) this._buffer.ReadByte ();
    }

    public int Read (byte [] byteBuffer, int offset, int count)
    {
        return this._buffer.Read (byteBuffer, offset, count);
    }

    public void WriteByte (byte b)
    {
        this._buffer.WriteByte (b);
    }

    public void Write (byte [] bytesToWrite)
    {
        this.Write (bytesToWrite, 0, bytesToWrite.Length);
    }

    public void Write (byte [] bytesToWrite, int offset, int countToWrite)
    {
        this._buffer.Write (bytesToWrite, offset, countToWrite);
    }

    public int ReadNBytes ()
    {
        byte c = this.ReadByte ();

        if (c < 1 || c > 4)
            throw new MySqlException (Resources.IncorrectTransmission);

        return this.ReadInteger (c);
    }

    public void SetByte (long position, byte value)
    {
        long currentPosition = this._buffer.Position;
        this._buffer.Position = position;
        this._buffer.WriteByte (value);
        this._buffer.Position = currentPosition;
    }

#endregion

#region Integer methods

    public long ReadFieldLength ()
    {
        byte c = this.ReadByte ();

        switch (c)
        {
            case 251: return -1;
            case 252: return this.ReadInteger (2);
            case 253: return this.ReadInteger (3);
            case 254: return this.ReadLong (8);
            default:  return c;
        }
    }

    public ulong ReadBitValue (int numbytes)
    {
        ulong value = 0;

        int     pos   = (int) this._buffer.Position;
        byte [] bits  = this._buffer.GetBuffer ();
        int     shift = 0;

        for (int i = 0; i < numbytes; i++)
        {
            value <<= shift;
            value |=  bits [pos++];
            shift =   8;
        }

        this._buffer.Position += numbytes;
        return value;
    }

    public long ReadLong (int numbytes)
    {
        Debug.Assert (this._buffer.Position + numbytes <= this._buffer.Length);

        byte [] bits = this._buffer.GetBuffer ();
        int     pos  = (int) this._buffer.Position;
        this._buffer.Position += numbytes;

        switch (numbytes)
        {
            case 2: return BitConverter.ToUInt16 (bits, pos);
            case 4: return BitConverter.ToUInt32 (bits, pos);
            case 8: return BitConverter.ToInt64 (bits, pos);
        }

        throw new NotSupportedException ("Only byte lengths of 2, 4, or 8 are supported");
    }

    public ulong ReadULong (int numbytes)
    {
        Debug.Assert (this._buffer.Position + numbytes <= this._buffer.Length);

        byte [] bits = this._buffer.GetBuffer ();

        int pos = (int) this._buffer.Position;
        this._buffer.Position += numbytes;

        switch (numbytes)
        {
            case 2: return BitConverter.ToUInt16 (bits, pos);
            case 4: return BitConverter.ToUInt32 (bits, pos);
            case 8: return BitConverter.ToUInt64 (bits, pos);
        }

        throw new NotSupportedException ("Only byte lengths of 2, 4, or 8 are supported");
    }

    public int Read3ByteInt ()
    {
        int value = 0;

        int     pos   = (int) this._buffer.Position;
        byte [] bits  = this._buffer.GetBuffer ();
        int     shift = 0;

        for (int i = 0; i < 3; i++)
        {
            value |= (int) (bits [pos++] << shift);
            shift += 8;
        }

        this._buffer.Position += 3;
        return value;
    }

    public int ReadInteger (int numbytes)
    {
        if (numbytes == 3)
            return this.Read3ByteInt ();

        Debug.Assert (numbytes <= 4);
        return (int) this.ReadLong (numbytes);
    }

    /// <summary>
    /// WriteInteger
    /// </summary>
    /// <param name="v"></param>
    /// <param name="numbytes"></param>
    public void WriteInteger (long v, int numbytes)
    {
        long val = v;

        Debug.Assert (numbytes > 0 && numbytes < 9);

        for (int x = 0; x < numbytes; x++)
        {
            this._tempBuffer [x] =   (byte) (val & 0xff);
            val                  >>= 8;
        }

        this.Write (this._tempBuffer, 0, numbytes);
    }

    public int ReadPackedInteger ()
    {
        byte c = this.ReadByte ();

        switch (c)
        {
            case 251: return -1;
            case 252: return this.ReadInteger (2);
            case 253: return this.ReadInteger (3);
            case 254: return this.ReadInteger (4);
            default:  return c;
        }
    }

    public void WriteLength (long length)
    {
        if (length < 251)
        {
            this.WriteByte ((byte) length);
        }
        else if (length < 65536L)
        {
            this.WriteByte (252);
            this.WriteInteger (length, 2);
        }
        else if (length < 16777216L)
        {
            this.WriteByte (253);
            this.WriteInteger (length, 3);
        }
        else
        {
            this.WriteByte (254);
            this.WriteInteger (length, 8);
        }
    }

#endregion

#region String methods

    public void WriteLenString (string s)
    {
        byte [] bytes = this._encoding.GetBytes (s);
        this.WriteLength (bytes.Length);
        this.Write (bytes, 0, bytes.Length);
    }

    public void WriteStringNoNull (string v)
    {
        byte [] bytes = this._encoding.GetBytes (v);
        this.Write (bytes, 0, bytes.Length);
    }

    public void WriteString (string v)
    {
        this.WriteStringNoNull (v);
        this.WriteByte (0);
    }

    public string ReadLenString ()
    {
        long len = this.ReadPackedInteger ();
        return this.ReadString (len);
    }

    public string ReadAsciiString (long length)
    {
        if (length == 0)
            return string.Empty;

        //            byte[] buf = new byte[length];
        this.Read (this._tempBuffer, 0, (int) length);
        return Encoding.GetEncoding ("us-ascii").GetString (this._tempBuffer, 0, (int) length);
        //return encoding.GetString(tempBuffer, 0, (int)length); //buf.Length);
    }

    public string ReadString (long length)
    {
        if (length == 0)
            return string.Empty;

        if (this._tempBuffer == null || length > this._tempBuffer.Length)
            this._tempBuffer = new byte[length];

        this.Read (this._tempBuffer, 0, (int) length);
        return this._encoding.GetString (this._tempBuffer, 0, (int) length);
    }

    public string ReadString ()
    {
        return this.ReadString (this._encoding);
    }

    public string ReadString (Encoding theEncoding)
    {
        byte [] bytes = this.ReadStringAsBytes ();
        string  s     = theEncoding.GetString (bytes, 0, bytes.Length);
        return s;
    }

    public byte [] ReadStringAsBytes ()
    {
        byte [] readBytes;
        byte [] bits       = this._buffer.GetBuffer ();
        int     end        = (int) this._buffer.Position;
        byte [] tempBuffer = bits;

        while (end < (int) this._buffer.Length &&
               tempBuffer [end] != 0 && (int) tempBuffer [end] != -1)
            end++;

        readBytes = new byte[end - this._buffer.Position];
        Array.Copy (tempBuffer, (int) this._buffer.Position, readBytes, 0, (int) (end - this._buffer.Position));
        this._buffer.Position = end + 1;

        return readBytes;
    }

#endregion
}
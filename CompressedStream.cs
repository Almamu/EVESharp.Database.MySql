// Copyright (c) 2004, 2019, Oracle and/or its affiliates. All rights reserved.
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
using System.IO;
using System.IO.Compression;
using EVESharp.Database.MySql.Common;
using System.Net;
using EVESharp.Database.MySql;

namespace EVESharp.Database.MySql;

/// <summary>
/// Summary description for CompressedStream.
/// </summary>
internal class CompressedStream : Stream
{
    // writing fields
    private Stream       baseStream;
    private MemoryStream cache;

    // reading fields
    private byte []       localByte;
    private byte []       inBuffer;
    private byte []       lengthBytes;
    private WeakReference inBufferRef;
    private int           inPos;
    private int           maxInPos;
    private DeflateStream compInStream;

    public CompressedStream (Stream baseStream)
    {
        this.baseStream  = baseStream;
        this.localByte   = new byte[1];
        this.lengthBytes = new byte[7];
        this.cache       = new MemoryStream ();
        this.inBufferRef = new WeakReference (this.inBuffer, false);
    }

#region Properties

    public override bool CanRead => this.baseStream.CanRead;

    public override bool CanWrite => this.baseStream.CanWrite;

    public override bool CanSeek => this.baseStream.CanSeek;

    public override long Length => this.baseStream.Length;

    public override long Position
    {
        get => this.baseStream.Position;
        set => this.baseStream.Position = value;
    }

#endregion

    public override void Close ()
    {
        base.Close ();
        this.baseStream.Close ();
        this.cache.Dispose ();
    }

    public override void SetLength (long value)
    {
        throw new NotSupportedException (Resources.CSNoSetLength);
    }

    public override int ReadByte ()
    {
        try
        {
            this.Read (this.localByte, 0, 1);
            return this.localByte [0];
        }
        catch (EndOfStreamException)
        {
            return -1;
        }
    }

    public override bool CanTimeout => this.baseStream.CanTimeout;

    public override int ReadTimeout
    {
        get => this.baseStream.ReadTimeout;
        set => this.baseStream.ReadTimeout = value;
    }

    public override int WriteTimeout
    {
        get => this.baseStream.WriteTimeout;
        set => this.baseStream.WriteTimeout = value;
    }

    public override int Read (byte [] buffer, int offset, int count)
    {
        if (buffer == null)
            throw new ArgumentNullException (nameof (buffer), Resources.BufferCannotBeNull);

        if (offset < 0 || offset >= buffer.Length)
            throw new ArgumentOutOfRangeException (nameof (offset), Resources.OffsetMustBeValid);

        if (offset + count > buffer.Length)
            throw new ArgumentException (Resources.BufferNotLargeEnough, nameof (buffer));

        if (this.inPos == this.maxInPos)
            this.PrepareNextPacket ();

        int countToRead = Math.Min (count, this.maxInPos - this.inPos);
        int countRead;

        if (this.compInStream != null)
            countRead = this.compInStream.Read (buffer, offset, countToRead);
        else
            countRead = this.baseStream.Read (buffer, offset, countToRead);

        this.inPos += countRead;

        // release the weak reference
        if (this.inPos == this.maxInPos)
        {
            this.compInStream = null;

            if (!Platform.IsMono ())
            {
                this.inBufferRef = new WeakReference (this.inBuffer, false);
                this.inBuffer    = null;
            }
        }

        return countRead;
    }

    private void PrepareNextPacket ()
    {
        MySqlStream.ReadFully (this.baseStream, this.lengthBytes, 0, 7);
        int compressedLength = this.lengthBytes [0] + (this.lengthBytes [1] << 8) + (this.lengthBytes [2] << 16);

        // lengthBytes[3] is seq
        int unCompressedLength = this.lengthBytes [4] + (this.lengthBytes [5] << 8) +
                                 (this.lengthBytes [6] << 16);

        if (unCompressedLength == 0)
        {
            unCompressedLength = compressedLength;
            this.compInStream  = null;
        }
        else
        {
            this.ReadNextPacket (compressedLength);
            MemoryStream ms = new MemoryStream (this.inBuffer, 2, compressedLength - 2);
            this.compInStream = new DeflateStream (ms, CompressionMode.Decompress);
        }

        this.inPos    = 0;
        this.maxInPos = unCompressedLength;
    }

    private void ReadNextPacket (int len)
    {
        this.inBuffer = this.inBufferRef.Target as byte [];

        if (this.inBuffer == null || this.inBuffer.Length < len)
            this.inBuffer = new byte[len];

        MySqlStream.ReadFully (this.baseStream, this.inBuffer, 0, len);
    }

    private MemoryStream CompressCache ()
    {
        // small arrays almost never yeild a benefit from compressing
        if (this.cache.Length < 50)
            return null;

        byte [] cacheBytes = this.cache.GetBuffer ();

        MemoryStream compressedBuffer = new MemoryStream ();

        compressedBuffer.WriteByte (0x78);
        compressedBuffer.WriteByte (0x9c);
        DeflateStream outCompStream = new DeflateStream (compressedBuffer, CompressionMode.Compress, true);
        outCompStream.Write (cacheBytes, 0, (int) this.cache.Length);
        outCompStream.Dispose ();
        int adler = IPAddress.HostToNetworkOrder (this.Adler32 (cacheBytes, 0, (int) this.cache.Length));
        compressedBuffer.Write (BitConverter.GetBytes (adler), 0, sizeof (uint));

        // if the compression hasn't helped, then just return null
        if (compressedBuffer.Length >= this.cache.Length)
            return null;

        return compressedBuffer;
    }

    private int Adler32 (byte [] bytes, int index, int length)
    {
        const uint a32mod = 65521;
        uint       s1     = 1, s2 = 0;

        for (int i = index; i < length; i++)
        {
            byte b = bytes [i];
            s1 = (s1 + b) % a32mod;
            s2 = (s2 + s1) % a32mod;
        }

        return unchecked ((int) ((s2 << 16) + s1));
    }

    private void CompressAndSendCache ()
    {
        long compressedLength, uncompressedLength;

        // we need to save the sequence byte that is written
        byte [] cacheBuffer = this.cache.GetBuffer ();

        byte seq = cacheBuffer [3];
        cacheBuffer [3] = 0;

        // first we compress our current cache
        MemoryStream compressedBuffer = this.CompressCache ();

        // now we set our compressed and uncompressed lengths
        // based on if our compression is going to help or not
        MemoryStream memStream;

        if (compressedBuffer == null)
        {
            compressedLength   = this.cache.Length;
            uncompressedLength = 0;
            memStream          = this.cache;
        }
        else
        {
            compressedLength   = compressedBuffer.Length;
            uncompressedLength = this.cache.Length;
            memStream          = compressedBuffer;
        }

        // Make space for length prefix (7 bytes) at the start of output
        long dataLength   = memStream.Length;
        int  bytesToWrite = (int) dataLength + 7;
        memStream.SetLength (bytesToWrite);
        byte [] buffer = memStream.GetBuffer ();
        Array.Copy (buffer, 0, buffer, 7, (int) dataLength);

        // Write length prefix
        buffer [0] = (byte) (compressedLength & 0xff);
        buffer [1] = (byte) ((compressedLength >> 8) & 0xff);
        buffer [2] = (byte) ((compressedLength >> 16) & 0xff);
        buffer [3] = seq;
        buffer [4] = (byte) (uncompressedLength & 0xff);
        buffer [5] = (byte) ((uncompressedLength >> 8) & 0xff);
        buffer [6] = (byte) ((uncompressedLength >> 16) & 0xff);

        this.baseStream.Write (buffer, 0, bytesToWrite);
        this.baseStream.Flush ();
        this.cache.SetLength (0);

        compressedBuffer?.Dispose ();
    }

    public override void Flush ()
    {
        if (!this.InputDone ())
            return;

        this.CompressAndSendCache ();
    }

    private bool InputDone ()
    {
        // if we have not done so yet, see if we can calculate how many bytes we are expecting
        if (this.baseStream is TimedStream && ((TimedStream) this.baseStream).IsClosed)
            return false;

        if (this.cache.Length < 4)
            return false;

        byte [] buf         = this.cache.GetBuffer ();
        int     expectedLen = buf [0] + (buf [1] << 8) + (buf [2] << 16);

        if (this.cache.Length < expectedLen + 4)
            return false;

        return true;
    }

    public override void WriteByte (byte value)
    {
        this.cache.WriteByte (value);
    }

    public override void Write (byte [] buffer, int offset, int count)
    {
        this.cache.Write (buffer, offset, count);
    }

    public override long Seek (long offset, SeekOrigin origin)
    {
        return this.baseStream.Seek (offset, origin);
    }
}
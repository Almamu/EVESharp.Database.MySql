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
using System.IO;
using System.Net.Sockets;
using System.Text;
using EVESharp.Database.MySql;

namespace EVESharp.Database.MySql;

/// <summary>
/// Summary description for MySqlStream.
/// </summary>
internal class MySqlStream
{
    private byte        sequenceByte;
    private int         maxBlockSize;
    private ulong       maxPacketSize;
    private byte []     packetHeader = new byte[4];
    private MySqlPacket packet;
    private TimedStream timedStream;
    private Stream      inStream;
    private Stream      outStream;
    private Socket      socket;
    public Socket Socket
    {
        get => this.socket;
        set => this.socket = value;
    }

    internal Stream BaseStream => this.timedStream;

    public MySqlStream (Encoding encoding)
    {
        // we have no idea what the real value is so we start off with the max value
        // The real value will be set in NativeDriver.Configure()
        this.maxPacketSize = ulong.MaxValue;

        // we default maxBlockSize to MaxValue since we will get the 'real' value in 
        // the authentication handshake and we know that value will not exceed 
        // true maxBlockSize prior to that.
        this.maxBlockSize = int.MaxValue;

        this.packet = new MySqlPacket (encoding);
    }

    public MySqlStream (Stream baseStream, Encoding encoding, bool compress, Socket pSocket = null)
        : this (encoding)
    {
        this.timedStream = new TimedStream (baseStream);
        Stream stream;

        if (compress)
            stream = new CompressedStream (this.timedStream);
        else
            stream = this.timedStream;

        this.inStream  = stream;
        this.outStream = stream;
        this.socket    = pSocket;
    }

    public void Close ()
    {
        this.outStream.Dispose ();
        this.inStream.Dispose ();
        this.timedStream.Close ();
    }

#region Properties

    public Encoding Encoding
    {
        get => this.packet.Encoding;
        set => this.packet.Encoding = value;
    }

    public void ResetTimeout (int timeout)
    {
        this.timedStream.ResetTimeout (timeout);
    }

    public byte SequenceByte
    {
        get => this.sequenceByte;
        set => this.sequenceByte = value;
    }

    public int MaxBlockSize
    {
        get => this.maxBlockSize;
        set => this.maxBlockSize = value;
    }

    public ulong MaxPacketSize
    {
        get => this.maxPacketSize;
        set => this.maxPacketSize = value;
    }

#endregion

#region Packet methods

    /// <summary>
    /// ReadPacket is called by NativeDriver to start reading the next
    /// packet on the stream.
    /// </summary>
    public MySqlPacket ReadPacket ()
    {
        //Debug.Assert(packet.Position == packet.Length);

        // make sure we have read all the data from the previous packet
        //Debug.Assert(HasMoreData == false, "HasMoreData is true in OpenPacket");

        this.LoadPacket ();

        // now we check if this packet is a server error
        if (this.packet.Buffer [0] == 0xff)
        {
            this.packet.ReadByte (); // read off the 0xff

            int    code = this.packet.ReadInteger (2);
            string msg  = string.Empty;

            if (this.packet.Version.isAtLeast (5, 5, 0))
                msg = this.packet.ReadString (Encoding.UTF8);
            else
                msg = this.packet.ReadString ();

            if (msg.StartsWith ("#", StringComparison.Ordinal))
            {
                msg.Substring (1, 5); /* state code */
                msg = msg.Substring (6);
            }

            switch (code)
            {
                case 4031: throw new MySqlException (msg, code, true);
                default:   throw new MySqlException (msg, code);
            }
        }

        return this.packet;
    }

    /// <summary>
    /// Reads the specified number of bytes from the stream and stores them at given 
    /// offset in the buffer.
    /// Throws EndOfStreamException if not all bytes can be read.
    /// </summary>
    /// <param name="stream">Stream to read from</param>
    /// <param name="buffer"> Array to store bytes read from the stream </param>
    /// <param name="offset">The offset in buffer at which to begin storing the data read from the current stream. </param>
    /// <param name="count">Number of bytes to read</param>
    internal static void ReadFully (Stream stream, byte [] buffer, int offset, int count)
    {
        int numRead   = 0;
        int numToRead = count;

        while (numToRead > 0)
        {
            int read = stream.Read (buffer, offset + numRead, numToRead);

            if (read == 0)
                throw new EndOfStreamException ();

            numRead   += read;
            numToRead -= read;
        }
    }

    /// <summary>
    /// LoadPacket loads up and decodes the header of the incoming packet.
    /// </summary>
    public void LoadPacket ()
    {
        try
        {
            this.packet.Length = 0;
            int offset = 0;

            while (true)
            {
                ReadFully (this.inStream, this.packetHeader, 0, 4);
                this.sequenceByte = (byte) (this.packetHeader [3] + 1);

                int length = (int) (this.packetHeader [0] + (this.packetHeader [1] << 8) +
                                    (this.packetHeader [2] << 16));

                // make roo for the next block
                this.packet.Length += length;
                ReadFully (this.inStream, this.packet.Buffer, offset, length);
                offset += length;

                // if this block was < maxBlock then it's last one in a multipacket series
                if (length < this.maxBlockSize)
                    break;
            }

            this.packet.Position = 0;
        }
        catch (IOException ioex)
        {
            throw new MySqlException (Resources.ReadFromStreamFailed, true, ioex);
        }
    }

    public void SendPacket (MySqlPacket packet)
    {
        byte [] buffer = packet.Buffer;
        int     length = packet.Position - 4;

        if ((ulong) length > this.maxPacketSize)
            throw new MySqlException (Resources.QueryTooLarge, (int) MySqlErrorCode.PacketTooLarge);

        int offset = 0;

        do
        {
            int lenToSend = length > this.maxBlockSize ? this.maxBlockSize : length;
            buffer [offset]     = (byte) (lenToSend & 0xff);
            buffer [offset + 1] = (byte) ((lenToSend >> 8) & 0xff);
            buffer [offset + 2] = (byte) ((lenToSend >> 16) & 0xff);
            buffer [offset + 3] = this.sequenceByte++;

            if (this.Socket != null && this.Socket.Available > 0)
                this.ReadPacket ();

            this.outStream.Write (buffer, offset, lenToSend + 4);
            this.outStream.Flush ();
            length -= lenToSend;
            offset += lenToSend;
        }
        while (length > 0);
    }

    public void SendEntirePacketDirectly (byte [] buffer, int count)
    {
        buffer [0] = (byte) (count & 0xff);
        buffer [1] = (byte) ((count >> 8) & 0xff);
        buffer [2] = (byte) ((count >> 16) & 0xff);
        buffer [3] = this.sequenceByte++;
        this.outStream.Write (buffer, 0, count + 4);
        this.outStream.Flush ();
    }

#endregion
}
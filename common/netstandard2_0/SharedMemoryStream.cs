// Copyright (c) 2014, 2021, Oracle and/or its affiliates.
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

using EVESharp.Database.MySql;
using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;

namespace EVESharp.Database.MySql.Common;

/// <summary>
/// Helper class to encapsulate shared memory functionality
/// Also cares of proper cleanup of file mapping object and cew
/// </summary>
internal class SharedMemory : IDisposable
{
    private const uint FILE_MAP_WRITE = 0x0002;

    private IntPtr fileMapping;
    private IntPtr view;

    public SharedMemory (string name, IntPtr size)
    {
        this.fileMapping = NativeMethods.OpenFileMapping (
            FILE_MAP_WRITE, false,
            name
        );

        if (this.fileMapping == IntPtr.Zero)
            throw new MySqlException ("Cannot open file mapping " + name);

        this.view = NativeMethods.MapViewOfFile (this.fileMapping, FILE_MAP_WRITE, 0, 0, size);
    }

#region Destructor

    ~SharedMemory ()
    {
        this.Dispose (false);
    }

#endregion

    public IntPtr View => this.view;

    public void Dispose ()
    {
        this.Dispose (true);
        GC.SuppressFinalize (this);
    }

    protected virtual void Dispose (bool disposing)
    {
        if (disposing)
        {
            if (this.view != IntPtr.Zero)
            {
                NativeMethods.UnmapViewOfFile (this.view);
                this.view = IntPtr.Zero;
            }

            if (this.fileMapping != IntPtr.Zero)
            {
                // Free the handle
                NativeMethods.CloseHandle (this.fileMapping);
                this.fileMapping = IntPtr.Zero;
            }
        }
    }
}

/// <summary>
/// Summary description for SharedMemoryStream.
/// </summary>
internal class SharedMemoryStream : Stream
{
    private string          memoryName;
    private EventWaitHandle serverRead;
    private EventWaitHandle serverWrote;
    private EventWaitHandle clientRead;
    private EventWaitHandle clientWrote;
    private EventWaitHandle connectionClosed;
    private SharedMemory    data;
    private int             bytesLeft;
    private int             position;
    private int             connectNumber;

    private const int BUFFERLENGTH = 16004;

    private int readTimeout  = Timeout.Infinite;
    private int writeTimeout = Timeout.Infinite;

    public SharedMemoryStream (string memName)
    {
        this.memoryName = memName;
    }

    public void Open (uint timeOut)
    {
        if (this.connectionClosed != null)
            Debug.Assert (false, "Connection is already open");

        this.GetConnectNumber (timeOut);
        this.SetupEvents ();
    }

    public override void Close ()
    {
        if (this.connectionClosed != null)
        {
            bool isClosed = this.connectionClosed.WaitOne (0);

            if (!isClosed)
            {
                this.connectionClosed.Set ();
                this.connectionClosed.Close ();
            }

            this.connectionClosed = null;
            EventWaitHandle [] handles = {this.serverRead, this.serverWrote, this.clientRead, this.clientWrote};

            for (int i = 0; i < handles.Length; i++)
                if (handles [i] != null)
                    handles [i].Close ();

            if (this.data != null)
            {
                this.data.Dispose ();
                this.data = null;
            }
        }
    }

    private void GetConnectNumber (uint timeOut)
    {
        EventWaitHandle connectRequest;

        try
        {
            connectRequest =
                EventWaitHandle.OpenExisting (this.memoryName + "_CONNECT_REQUEST");
        }
        catch (Exception)
        {
            // If server runs as service, its shared memory is global 
            // And if connector runs in user session, it needs to prefix
            // shared memory name with "Global\"
            string prefixedMemoryName = @"Global\" + this.memoryName;

            connectRequest =
                EventWaitHandle.OpenExisting (prefixedMemoryName + "_CONNECT_REQUEST");

            this.memoryName = prefixedMemoryName;
        }

        EventWaitHandle connectAnswer =
            EventWaitHandle.OpenExisting (this.memoryName + "_CONNECT_ANSWER");

        using (SharedMemory connectData =
               new SharedMemory (this.memoryName + "_CONNECT_DATA", (IntPtr) 4))
        {
            // now start the connection
            if (!connectRequest.Set ())
                throw new MySqlException ("Failed to open shared memory connection");

            if (!connectAnswer.WaitOne ((int) (timeOut * 1000), false))
                throw new MySqlException ("Timeout during connection");

            this.connectNumber = Marshal.ReadInt32 (connectData.View);
        }
    }

    private void SetupEvents ()
    {
        string prefix = this.memoryName + "_" + this.connectNumber;
        this.data             = new SharedMemory (prefix + "_DATA", (IntPtr) BUFFERLENGTH);
        this.serverWrote      = EventWaitHandle.OpenExisting (prefix + "_SERVER_WROTE");
        this.serverRead       = EventWaitHandle.OpenExisting (prefix + "_SERVER_READ");
        this.clientWrote      = EventWaitHandle.OpenExisting (prefix + "_CLIENT_WROTE");
        this.clientRead       = EventWaitHandle.OpenExisting (prefix + "_CLIENT_READ");
        this.connectionClosed = EventWaitHandle.OpenExisting (prefix + "_CONNECTION_CLOSED");

        // tell the server we are ready
        this.serverRead.Set ();
    }

#region Properties

    public override bool CanRead => true;

    public override bool CanSeek => false;

    public override bool CanWrite => true;

    public override long Length => throw new NotSupportedException ("SharedMemoryStream does not support seeking - length");

    public override long Position
    {
        get => throw new NotSupportedException ("SharedMemoryStream does not support seeking - position");
        set { }
    }

#endregion

    public override void Flush ()
    {
        // No need to flush anything to disk ,as our shared memory is backed 
        // by the page file
    }

    public override int Read (byte [] buffer, int offset, int count)
    {
        int                    timeLeft    = this.readTimeout;
        WaitHandle []          waitHandles = {this.serverWrote, this.connectionClosed};
        LowResolutionStopwatch stopwatch   = new LowResolutionStopwatch ();

        while (this.bytesLeft == 0)
        {
            stopwatch.Start ();
            int index = WaitHandle.WaitAny (waitHandles, timeLeft);
            stopwatch.Stop ();

            if (index == WaitHandle.WaitTimeout)
                throw new TimeoutException ("Timeout when reading from shared memory");

            if (waitHandles [index] == this.connectionClosed)
                throw new MySqlException ("Connection to server lost", true, null);

            if (this.readTimeout != Timeout.Infinite)
            {
                timeLeft = this.readTimeout - (int) stopwatch.ElapsedMilliseconds;

                if (timeLeft < 0)
                    throw new TimeoutException ("Timeout when reading from shared memory");
            }

            this.bytesLeft = Marshal.ReadInt32 (this.data.View);
            this.position  = 4;
        }

        int  len     = Math.Min (count, this.bytesLeft);
        long baseMem = this.data.View.ToInt64 () + this.position;

        for (int i = 0; i < len; i++, this.position++)
            buffer [offset + i] = Marshal.ReadByte ((IntPtr) (baseMem + i));

        this.bytesLeft -= len;

        if (this.bytesLeft == 0)
            this.clientRead.Set ();

        return len;
    }

    public override long Seek (long offset, SeekOrigin origin)
    {
        throw new NotSupportedException ("SharedMemoryStream does not support seeking");
    }

    public override void Write (byte [] buffer, int offset, int count)
    {
        int                    leftToDo    = count;
        int                    buffPos     = offset;
        WaitHandle []          waitHandles = {this.serverRead, this.connectionClosed};
        LowResolutionStopwatch stopwatch   = new LowResolutionStopwatch ();
        int                    timeLeft    = this.writeTimeout;

        while (leftToDo > 0)
        {
            stopwatch.Start ();
            int index = WaitHandle.WaitAny (waitHandles, timeLeft);
            stopwatch.Stop ();

            if (waitHandles [index] == this.connectionClosed)
                throw new MySqlException ("Connection to server lost", true, null);

            if (index == WaitHandle.WaitTimeout)
                throw new TimeoutException ("Timeout when reading from shared memory");

            if (this.writeTimeout != Timeout.Infinite)
            {
                timeLeft = this.writeTimeout - (int) stopwatch.ElapsedMilliseconds;

                if (timeLeft < 0)
                    throw new TimeoutException ("Timeout when writing to shared memory");
            }

            int  bytesToDo = Math.Min (leftToDo, BUFFERLENGTH);
            long baseMem   = this.data.View.ToInt64 () + 4;
            Marshal.WriteInt32 (this.data.View, bytesToDo);
            Marshal.Copy (buffer, buffPos, (IntPtr) baseMem, bytesToDo);
            buffPos  += bytesToDo;
            leftToDo -= bytesToDo;

            if (!this.clientWrote.Set ())
                throw new MySqlException ("Writing to shared memory failed");
        }
    }

    public override void SetLength (long value)
    {
        throw new NotSupportedException ("SharedMemoryStream does not support seeking");
    }

    public override bool CanTimeout => true;

    public override int ReadTimeout
    {
        get => this.readTimeout;
        set => this.readTimeout = value;
    }

    public override int WriteTimeout
    {
        get => this.writeTimeout;
        set => this.writeTimeout = value;
    }
}
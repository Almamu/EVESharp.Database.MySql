// Copyright (c) 2009, 2019, Oracle and/or its affiliates. All rights reserved.
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

using EVESharp.Database.MySql.Common;
using System;
using System.IO;

namespace EVESharp.Database.MySql;

/// <summary>
/// Stream that supports timeout of IO operations.
/// This class is used is used to support timeouts for SQL command, where a 
/// typical operation involves several network reads/writes. 
/// Timeout here is defined as the accumulated duration of all IO operations.
/// </summary>
internal class TimedStream : Stream
{
    private readonly Stream _baseStream;

    private          int                    _timeout;
    private          int                    _lastReadTimeout;
    private          int                    _lastWriteTimeout;
    private readonly LowResolutionStopwatch _stopwatch;

    internal bool IsClosed { get; private set; }

    private enum IOKind
    {
        Read,
        Write
    };

    /// <summary>
    /// Construct a TimedStream
    /// </summary>
    /// <param name="baseStream"> Undelying stream</param>
    public TimedStream (Stream baseStream)
    {
        this._baseStream = baseStream;
        this._timeout    = baseStream.CanTimeout ? baseStream.ReadTimeout : System.Threading.Timeout.Infinite;
        this.IsClosed    = false;
        this._stopwatch  = new LowResolutionStopwatch ();
    }

    /// <summary>
    /// Figure out whether it is necessary to reset timeout on stream.
    /// We track the current value of timeout and try to avoid
    /// changing it too often, because setting Read/WriteTimeout property
    /// on network stream maybe a slow operation that involves a system call 
    /// (setsockopt). Therefore, we allow a small difference, and do not 
    /// reset timeout if current value is slightly greater than the requested
    /// one (within 0.1 second).
    /// </summary>
    private bool ShouldResetStreamTimeout (int currentValue, int newValue)
    {
        if (!this._baseStream.CanTimeout)
            return false;

        if (newValue == System.Threading.Timeout.Infinite
            && currentValue != newValue)
            return true;

        if (newValue > currentValue)
            return true;

        return currentValue >= newValue + 100;
    }

    private void StartTimer (IOKind op)
    {
        int streamTimeout;

        if (this._timeout == System.Threading.Timeout.Infinite)
            streamTimeout = System.Threading.Timeout.Infinite;
        else
            streamTimeout = this._timeout - (int) this._stopwatch.ElapsedMilliseconds;

        if (op == IOKind.Read)
        {
            if (this.ShouldResetStreamTimeout (this._lastReadTimeout, streamTimeout))
            {
                this._baseStream.ReadTimeout = streamTimeout;
                this._lastReadTimeout        = streamTimeout;
            }
        }
        else
        {
            if (this.ShouldResetStreamTimeout (this._lastWriteTimeout, streamTimeout))
            {
                this._baseStream.WriteTimeout = streamTimeout;
                this._lastWriteTimeout        = streamTimeout;
            }
        }

        if (this._timeout == System.Threading.Timeout.Infinite)
            return;

        this._stopwatch.Start ();
    }

    private void StopTimer ()
    {
        if (this._timeout == System.Threading.Timeout.Infinite)
            return;

        this._stopwatch.Stop ();

        // Normally, a timeout exception would be thrown  by stream itself, 
        // since we set the read/write timeout  for the stream.  However 
        // there is a gap between  end of IO operation and stopping the 
        // stop watch,  and it makes it possible for timeout to exceed 
        // even after IO completed successfully.
        if (this._stopwatch.ElapsedMilliseconds > this._timeout)
        {
            this.ResetTimeout (System.Threading.Timeout.Infinite);
            throw new TimeoutException ("Timeout in IO operation");
        }
    }

    public override bool CanRead => this._baseStream.CanRead;

    public override bool CanSeek => this._baseStream.CanSeek;

    public override bool CanWrite => this._baseStream.CanWrite;

    public override void Flush ()
    {
        try
        {
            this.StartTimer (IOKind.Write);
            this._baseStream.Flush ();
            this.StopTimer ();
        }
        catch (Exception e)
        {
            this.HandleException (e);
            throw;
        }
    }

    public override long Length => this._baseStream.Length;

    public override long Position
    {
        get => this._baseStream.Position;
        set => this._baseStream.Position = value;
    }

    public override int Read (byte [] buffer, int offset, int count)
    {
        try
        {
            this.StartTimer (IOKind.Read);
            int retval = this._baseStream.Read (buffer, offset, count);
            this.StopTimer ();
            return retval;
        }
        catch (Exception e)
        {
            this.HandleException (e);
            throw;
        }
    }

    public override int ReadByte ()
    {
        try
        {
            this.StartTimer (IOKind.Read);
            int retval = this._baseStream.ReadByte ();
            this.StopTimer ();
            return retval;
        }
        catch (Exception e)
        {
            this.HandleException (e);
            throw;
        }
    }

    public override long Seek (long offset, SeekOrigin origin)
    {
        return this._baseStream.Seek (offset, origin);
    }

    public override void SetLength (long value)
    {
        this._baseStream.SetLength (value);
    }

    public override void Write (byte [] buffer, int offset, int count)
    {
        try
        {
            this.StartTimer (IOKind.Write);
            this._baseStream.Write (buffer, offset, count);
            this.StopTimer ();
        }
        catch (Exception e)
        {
            this.HandleException (e);
            throw;
        }
    }

    public override bool CanTimeout => this._baseStream.CanTimeout;

    public override int ReadTimeout
    {
        get => this._baseStream.ReadTimeout;
        set => this._baseStream.ReadTimeout = value;
    }
    public override int WriteTimeout
    {
        get => this._baseStream.WriteTimeout;
        set => this._baseStream.WriteTimeout = value;
    }

    public override void Close ()
    {
        if (this.IsClosed)
            return;

        this.IsClosed = true;
        this._baseStream.Close ();
        this._baseStream.Dispose ();
    }

    public void ResetTimeout (int newTimeout)
    {
        if (newTimeout == System.Threading.Timeout.Infinite || newTimeout == 0)
            this._timeout = System.Threading.Timeout.Infinite;
        else
            this._timeout = newTimeout;

        this._stopwatch.Reset ();
    }

    /// <summary>
    /// Common handler for IO exceptions.
    /// Resets timeout to infinity if timeout exception is 
    /// detected and stops the times.
    /// </summary>
    /// <param name="e">original exception</param>
    private void HandleException (Exception e)
    {
        this._stopwatch.Stop ();
        this.ResetTimeout (-1);
    }
}
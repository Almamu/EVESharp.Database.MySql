// Copyright (c) 2009, 2022, Oracle and/or its affiliates.
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

using Microsoft.Win32.SafeHandles;
using EVESharp.Database.MySql.Common;
using System;
using System.ComponentModel;
using System.IO;
using System.Runtime.InteropServices;
using System.Security;
using System.Threading;
using EVESharp.Database.MySql;

namespace EVESharp.Database.MySql.Common;

/// <summary>
/// Summary description for API.
/// </summary>
[SuppressUnmanagedCodeSecurity ()]
internal class NamedPipeStream : Stream
{
    private       SafeFileHandle handle;
    private       Stream         fileStream;
    private       int            readTimeout       = Timeout.Infinite;
    private       int            writeTimeout      = Timeout.Infinite;
    private const int            ERROR_PIPE_BUSY   = 231;
    private const int            ERROR_SEM_TIMEOUT = 121;

    public NamedPipeStream (string path, FileAccess mode, uint timeout)
    {
        this.Open (path, mode, timeout);
    }

    private void CancelIo ()
    {
        bool ok = NativeMethods.CancelIo (this.handle.DangerousGetHandle ());

        if (!ok)
            throw new Win32Exception (Marshal.GetLastWin32Error ());
    }

    public void Open (string path, FileAccess mode, uint timeout)
    {
        IntPtr nativeHandle;

        for (;;)
        {
            NativeMethods.SecurityAttributes security = new NativeMethods.SecurityAttributes ();
            security.inheritHandle = true;
            security.Length        = Marshal.SizeOf (security);

            nativeHandle = NativeMethods.CreateFile (
                path, NativeMethods.FILE_READ_ATTRIBUTES | NativeMethods.FILE_READ_DATA |
                      NativeMethods.FILE_WRITE_ATTRIBUTES | NativeMethods.FILE_WRITE_DATA,
                0, security, NativeMethods.OPEN_EXISTING, NativeMethods.FILE_FLAG_OVERLAPPED, 0
            );

            if (nativeHandle != IntPtr.Zero)
                break;

            if (Marshal.GetLastWin32Error () != ERROR_PIPE_BUSY)
                throw new Win32Exception (
                    Marshal.GetLastWin32Error (),
                    "Error opening pipe"
                );

            LowResolutionStopwatch sw      = LowResolutionStopwatch.StartNew ();
            bool                   success = NativeMethods.WaitNamedPipe (path, timeout);
            sw.Stop ();

            if (!success)
            {
                if (timeout < sw.ElapsedMilliseconds ||
                    Marshal.GetLastWin32Error () == ERROR_SEM_TIMEOUT)
                    throw new TimeoutException ("Timeout waiting for named pipe");
                else
                    throw new Win32Exception (
                        Marshal.GetLastWin32Error (),
                        "Error waiting for pipe"
                    );
            }

            timeout -= (uint) sw.ElapsedMilliseconds;
        }

        this.handle     = new SafeFileHandle (nativeHandle, true);
        this.fileStream = new FileStream (this.handle, mode, 4096, true);
    }

    public override bool CanRead => this.fileStream.CanRead;

    public override bool CanWrite => this.fileStream.CanWrite;

    public override bool CanSeek => throw new NotSupportedException (Resources.NamedPipeNoSeek);

    public override long Length => throw new NotSupportedException (Resources.NamedPipeNoSeek);

    public override long Position
    {
        get => throw new NotSupportedException (Resources.NamedPipeNoSeek);
        set { }
    }

    public override void Flush ()
    {
        this.fileStream.Flush ();
    }

    public override int Read (byte [] buffer, int offset, int count)
    {
        if (this.readTimeout == Timeout.Infinite)
            return this.fileStream.Read (buffer, offset, count);

        IAsyncResult result = this.fileStream.BeginRead (buffer, offset, count, null, null);

        if (result.CompletedSynchronously)
            return this.fileStream.EndRead (result);

        if (!result.AsyncWaitHandle.WaitOne (this.readTimeout))
        {
            this.CancelIo ();
            throw new TimeoutException ("Timeout in named pipe read");
        }

        return this.fileStream.EndRead (result);
    }

    public override void Write (byte [] buffer, int offset, int count)
    {
        if (this.writeTimeout == Timeout.Infinite)
        {
            this.fileStream.Write (buffer, offset, count);
            return;
        }

        IAsyncResult result = this.fileStream.BeginWrite (buffer, offset, count, null, null);

        if (result.CompletedSynchronously)
            this.fileStream.EndWrite (result);

        if (!result.AsyncWaitHandle.WaitOne (this.readTimeout))
        {
            this.CancelIo ();
            throw new TimeoutException ("Timeout in named pipe write");
        }

        this.fileStream.EndWrite (result);
    }

    public override void Close ()
    {
        if (this.handle != null && !this.handle.IsInvalid && !this.handle.IsClosed)
        {
            this.fileStream.Close ();

            try
            {
                this.handle.Close ();
            }
            catch (Exception) { }
        }
    }

    public override void SetLength (long length)
    {
        throw new NotSupportedException (Resources.NamedPipeNoSetLength);
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

    public override long Seek (long offset, SeekOrigin origin)
    {
        throw new NotSupportedException (Resources.NamedPipeNoSeek);
    }

    internal static Stream Create (string pipeName, string hostname, uint timeout)
    {
        string pipePath;

        if (0 == string.Compare (hostname, "localhost", true))
            pipePath = @"\\.\pipe\" + pipeName;
        else
            pipePath = string.Format (@"\\{0}\pipe\{1}", hostname, pipeName);

        return new NamedPipeStream (pipePath, FileAccess.ReadWrite, timeout);
    }
}
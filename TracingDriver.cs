// Copyright (c) 2009, 2021, Oracle and/or its affiliates.
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
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using EVESharp.Database.MySql;
using EVESharp.Database.MySql.Common;

namespace EVESharp.Database.MySql;

internal class TracingDriver : Driver
{
    private static long      driverCounter;
    private        long      driverId;
    private        ResultSet activeResult;
    private        int       rowSizeInBytes;

    public TracingDriver (MySqlConnectionStringBuilder settings)
        : base (settings)
    {
        this.driverId = Interlocked.Increment (ref driverCounter);
    }

    public override void Open ()
    {
        base.Open ();

        MySqlTrace.TraceEvent (
            TraceEventType.Information, MySqlTraceEventType.ConnectionOpened,
            Resources.TraceOpenConnection, this.driverId, this.Settings.ConnectionString, this.ThreadID
        );
    }

    public override void Close ()
    {
        base.Close ();

        MySqlTrace.TraceEvent (
            TraceEventType.Information, MySqlTraceEventType.ConnectionClosed,
            Resources.TraceCloseConnection, this.driverId
        );
    }

    public override void SendQuery (MySqlPacket p, int paramsPosition)
    {
        this.rowSizeInBytes = 0;
        string cmdText         = this.Encoding.GetString (p.Buffer, paramsPosition, p.Length - paramsPosition);
        string normalizedQuery = null;

        if (cmdText.Length > 300)
        {
            QueryNormalizer normalizer = new QueryNormalizer ();
            normalizedQuery = normalizer.Normalize (cmdText);
            cmdText         = cmdText.Substring (0, 300);
        }

        base.SendQuery (p, paramsPosition);

        MySqlTrace.TraceEvent (
            TraceEventType.Information, MySqlTraceEventType.QueryOpened,
            Resources.TraceQueryOpened, this.driverId, this.ThreadID, cmdText
        );

        if (normalizedQuery != null)
            MySqlTrace.TraceEvent (
                TraceEventType.Information, MySqlTraceEventType.QueryNormalized,
                Resources.TraceQueryNormalized, this.driverId, this.ThreadID, normalizedQuery
            );
    }

    protected override int GetResult (int statementId, ref int affectedRows, ref long insertedId)
    {
        try
        {
            int fieldCount = base.GetResult (statementId, ref affectedRows, ref insertedId);

            MySqlTrace.TraceEvent (
                TraceEventType.Information, MySqlTraceEventType.ResultOpened,
                Resources.TraceResult, this.driverId, fieldCount, affectedRows, insertedId
            );

            return fieldCount;
        }
        catch (MySqlException ex)
        {
            // we got an error so we report it
            MySqlTrace.TraceEvent (
                TraceEventType.Information, MySqlTraceEventType.Error,
                Resources.TraceOpenResultError, this.driverId, ex.Number, ex.Message
            );

            throw;
        }
    }

    public override ResultSet NextResult (int statementId, bool force)
    {
        // first let's see if we already have a resultset on this statementId
        if (this.activeResult != null)
        {
            //oldRS = activeResults[statementId];
            if (this.Settings.UseUsageAdvisor)
                this.ReportUsageAdvisorWarnings (statementId, this.activeResult);

            MySqlTrace.TraceEvent (
                TraceEventType.Information, MySqlTraceEventType.ResultClosed,
                Resources.TraceResultClosed, this.driverId, this.activeResult.TotalRows, this.activeResult.SkippedRows, this.rowSizeInBytes
            );

            this.rowSizeInBytes = 0;
            this.activeResult   = null;
        }

        this.activeResult = base.NextResult (statementId, force);
        return this.activeResult;
    }

    public override int PrepareStatement (string sql, ref MySqlField [] parameters)
    {
        int statementId = base.PrepareStatement (sql, ref parameters);

        MySqlTrace.TraceEvent (
            TraceEventType.Information, MySqlTraceEventType.StatementPrepared,
            Resources.TraceStatementPrepared, this.driverId, sql, statementId
        );

        return statementId;
    }

    public override void CloseStatement (int id)
    {
        base.CloseStatement (id);

        MySqlTrace.TraceEvent (
            TraceEventType.Information, MySqlTraceEventType.StatementClosed,
            Resources.TraceStatementClosed, this.driverId, id
        );
    }

    public override void SetDatabase (string dbName)
    {
        base.SetDatabase (dbName);

        MySqlTrace.TraceEvent (
            TraceEventType.Information, MySqlTraceEventType.NonQuery,
            Resources.TraceSetDatabase, this.driverId, dbName
        );
    }

    public override void ExecuteStatement (MySqlPacket packetToExecute)
    {
        base.ExecuteStatement (packetToExecute);
        int pos = packetToExecute.Position;
        packetToExecute.Position = 1;
        int statementId = packetToExecute.ReadInteger (4);
        packetToExecute.Position = pos;

        MySqlTrace.TraceEvent (
            TraceEventType.Information, MySqlTraceEventType.StatementExecuted,
            Resources.TraceStatementExecuted, this.driverId, statementId, this.ThreadID
        );
    }

    public override bool FetchDataRow (int statementId, int columns)
    {
        try
        {
            bool b = base.FetchDataRow (statementId, columns);

            if (b)
                this.rowSizeInBytes += (this.handler as NativeDriver).Packet.Length;

            return b;
        }
        catch (MySqlException ex)
        {
            MySqlTrace.TraceEvent (
                TraceEventType.Error, MySqlTraceEventType.Error,
                Resources.TraceFetchError, this.driverId, ex.Number, ex.Message
            );

            throw;
        }
    }

    public override void CloseQuery (MySqlConnection connection, int statementId)
    {
        base.CloseQuery (connection, statementId);

        MySqlTrace.TraceEvent (
            TraceEventType.Information, MySqlTraceEventType.QueryClosed,
            Resources.TraceQueryDone, this.driverId
        );
    }

    public override List <MySqlError> ReportWarnings (MySqlConnection connection)
    {
        List <MySqlError> warnings = base.ReportWarnings (connection);

        foreach (MySqlError warning in warnings)
            MySqlTrace.TraceEvent (
                TraceEventType.Warning, MySqlTraceEventType.Warning,
                Resources.TraceWarning, this.driverId, warning.Level, warning.Code, warning.Message
            );

        return warnings;
    }

    private bool AllFieldsAccessed (ResultSet rs)
    {
        if (rs.Fields == null || rs.Fields.Length == 0)
            return true;

        for (int i = 0; i < rs.Fields.Length; i++)
            if (!rs.FieldRead (i))
                return false;

        return true;
    }

    private void ReportUsageAdvisorWarnings (int statementId, ResultSet rs)
    {
        if (!this.Settings.UseUsageAdvisor)
            return;

        if (this.HasStatus (ServerStatusFlags.NoIndex))
            MySqlTrace.TraceEvent (
                TraceEventType.Warning, MySqlTraceEventType.UsageAdvisorWarning,
                Resources.TraceUAWarningNoIndex, this.driverId, UsageAdvisorWarningFlags.NoIndex
            );
        else if (this.HasStatus (ServerStatusFlags.BadIndex))
            MySqlTrace.TraceEvent (
                TraceEventType.Warning, MySqlTraceEventType.UsageAdvisorWarning,
                Resources.TraceUAWarningBadIndex, this.driverId, UsageAdvisorWarningFlags.BadIndex
            );

        // report abandoned rows
        if (rs.SkippedRows > 0)
            MySqlTrace.TraceEvent (
                TraceEventType.Warning, MySqlTraceEventType.UsageAdvisorWarning,
                Resources.TraceUAWarningSkippedRows, this.driverId, UsageAdvisorWarningFlags.SkippedRows, rs.SkippedRows
            );

        // report not all fields accessed
        if (!this.AllFieldsAccessed (rs))
        {
            StringBuilder notAccessed = new StringBuilder ("");
            string        delimiter   = "";

            for (int i = 0; i < rs.Size; i++)
                if (!rs.FieldRead (i))
                {
                    notAccessed.AppendFormat ("{0}{1}", delimiter, rs.Fields [i].ColumnName);
                    delimiter = ",";
                }

            MySqlTrace.TraceEvent (
                TraceEventType.Warning, MySqlTraceEventType.UsageAdvisorWarning,
                Resources.TraceUAWarningSkippedColumns, this.driverId, UsageAdvisorWarningFlags.SkippedColumns,
                notAccessed.ToString ()
            );
        }

        // report type conversions if any
        if (rs.Fields != null)
            foreach (MySqlField f in rs.Fields)
            {
                StringBuilder s         = new StringBuilder ();
                string        delimiter = "";

                foreach (Type t in f.TypeConversions)
                {
                    s.AppendFormat ("{0}{1}", delimiter, t.Name);
                    delimiter = ",";
                }

                if (s.Length > 0)
                    MySqlTrace.TraceEvent (
                        TraceEventType.Warning, MySqlTraceEventType.UsageAdvisorWarning,
                        Resources.TraceUAWarningFieldConversion, this.driverId, UsageAdvisorWarningFlags.FieldConversion,
                        f.ColumnName, s.ToString ()
                    );
            }
    }
}
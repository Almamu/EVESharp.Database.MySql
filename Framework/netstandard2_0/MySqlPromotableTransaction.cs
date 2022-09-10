// Copyright (c) 2004, 2020, Oracle and/or its affiliates. All rights reserved.
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
using System.Transactions;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;

namespace EVESharp.Database.MySql;

/// <summary>
/// Represents a single(not nested) TransactionScope
/// </summary>
internal class MySqlTransactionScope
{
    public MySqlConnection  connection;
    public Transaction      baseTransaction;
    public MySqlTransaction simpleTransaction;
    public int              rollbackThreadId;

    public MySqlTransactionScope
    (
        MySqlConnection  con, Transaction trans,
        MySqlTransaction simpleTransaction
    )
    {
        this.connection        = con;
        this.baseTransaction   = trans;
        this.simpleTransaction = simpleTransaction;
    }

    public void Rollback (SinglePhaseEnlistment singlePhaseEnlistment)
    {
        // prevent commands in main thread to run concurrently
        Driver driver = this.connection.driver;

        lock (driver)
        {
            this.rollbackThreadId = Thread.CurrentThread.ManagedThreadId;

            while (this.connection.Reader != null)
                // wait for reader to finish. Maybe we should not wait 
                // forever and cancel it after some time?
                Thread.Sleep (100);

            this.simpleTransaction.Rollback ();
            singlePhaseEnlistment.Aborted ();
            DriverTransactionManager.RemoveDriverInTransaction (this.baseTransaction);

            driver.currentTransaction = null;

            if (this.connection.State == ConnectionState.Closed)
                this.connection.CloseFully ();

            this.rollbackThreadId = 0;
        }
    }

    public void SinglePhaseCommit (SinglePhaseEnlistment singlePhaseEnlistment)
    {
        this.simpleTransaction.Commit ();
        singlePhaseEnlistment.Committed ();
        DriverTransactionManager.RemoveDriverInTransaction (this.baseTransaction);
        this.connection.driver.currentTransaction = null;

        if (this.connection.State == ConnectionState.Closed)
            this.connection.CloseFully ();
    }
}

internal sealed class MySqlPromotableTransaction : IPromotableSinglePhaseNotification, ITransactionPromoter
{
    // Per-thread stack to manage nested transaction scopes
    [ThreadStatic] private static Stack <MySqlTransactionScope> globalScopeStack;

    private MySqlConnection               connection;
    private Transaction                   baseTransaction;
    private Stack <MySqlTransactionScope> scopeStack;

    public MySqlPromotableTransaction (MySqlConnection connection, Transaction baseTransaction)
    {
        this.connection      = connection;
        this.baseTransaction = baseTransaction;
    }

    public Transaction BaseTransaction
    {
        get
        {
            if (this.scopeStack.Count > 0)
                return this.scopeStack.Peek ().baseTransaction;
            else
                return null;
        }
    }

    public bool InRollback
    {
        get
        {
            if (this.scopeStack.Count > 0)
            {
                MySqlTransactionScope currentScope = this.scopeStack.Peek ();

                if (currentScope.rollbackThreadId == Thread.CurrentThread.ManagedThreadId)
                    return true;
            }

            return false;
        }
    }

    void IPromotableSinglePhaseNotification.Initialize ()
    {
        string                     valueName         = Enum.GetName (typeof (System.Transactions.IsolationLevel), this.baseTransaction.IsolationLevel);
        System.Data.IsolationLevel dataLevel         = (System.Data.IsolationLevel) Enum.Parse (typeof (System.Data.IsolationLevel), valueName);
        MySqlTransaction           simpleTransaction = this.connection.BeginTransaction (dataLevel, "SESSION");

        // We need to save the per-thread scope stack locally.
        // We cannot always use thread static variable in rollback: when scope
        // times out, rollback is issued by another thread.
        if (globalScopeStack == null)
            globalScopeStack = new Stack <MySqlTransactionScope> ();

        this.scopeStack = globalScopeStack;

        this.scopeStack.Push (
            new MySqlTransactionScope (
                this.connection, this.baseTransaction,
                simpleTransaction
            )
        );
    }

    void IPromotableSinglePhaseNotification.Rollback (SinglePhaseEnlistment singlePhaseEnlistment)
    {
        MySqlTransactionScope current = this.scopeStack.Peek ();
        current.Rollback (singlePhaseEnlistment);
        this.scopeStack.Pop ();
    }

    void IPromotableSinglePhaseNotification.SinglePhaseCommit (SinglePhaseEnlistment singlePhaseEnlistment)
    {
        this.scopeStack.Pop ().SinglePhaseCommit (singlePhaseEnlistment);
    }

    byte [] ITransactionPromoter.Promote ()
    {
        throw new NotSupportedException ();
    }
}

internal class DriverTransactionManager
{
    private static Hashtable driversInUse = new Hashtable ();

    public static Driver GetDriverInTransaction (Transaction transaction)
    {
        lock (driversInUse.SyncRoot)
        {
            Driver d = (Driver) driversInUse [transaction.GetHashCode ()];
            return d;
        }
    }

    public static void SetDriverInTransaction (Driver driver)
    {
        lock (driversInUse.SyncRoot)
        {
            driversInUse [driver.currentTransaction.BaseTransaction.GetHashCode ()] = driver;
        }
    }

    public static void RemoveDriverInTransaction (Transaction transaction)
    {
        lock (driversInUse.SyncRoot)
        {
            driversInUse.Remove (transaction.GetHashCode ());
        }
    }
}
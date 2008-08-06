// Copyright (C) 2008 Xtensive LLC.
// All rights reserved.
// For conditions of distribution and use, see license.
// Created by: Dmitri Maximov
// Created:    2008.01.14

using System;
using System.Diagnostics;
using System.Transactions;
using Xtensive.Core;
using Xtensive.Core.Internals.DocTemplates;
using Xtensive.Integrity.Resources;

namespace Xtensive.Integrity.Transactions
{
  /// <summary>
  /// Base class for any transaction.
  /// </summary>
  /// <typeparam name="TScope">Actual scope type.</typeparam>
  /// <typeparam name="TTransaction">Actual transaction type.</typeparam>
  public abstract class TransactionBase<TScope, TTransaction> : Context<TScope>, 
    ITransaction<TScope>
    where TScope: TransactionScopeBase<TScope, TTransaction>
    where TTransaction: TransactionBase<TScope, TTransaction>
  {
    private readonly Guid identifier;
    private IsolationLevel isolationLevel;
    private TransactionState state = TransactionState.Active;

    #region IIdentified<Guid> Members

    /// <inheritdoc/>
    [DebuggerHidden]
    public Guid Identifier
    {
      get { return identifier; }
    }

    /// <inheritdoc/>
    [DebuggerHidden]
    object IIdentified.Identifier
    {
      get { return identifier; }
    }

    #endregion

    /// <inheritdoc/>
    [DebuggerHidden]
    public TransactionState State
    {
      get { return state; }
    }

    /// <inheritdoc/>
    [DebuggerHidden]
    public IsolationLevel IsolationLevel
    {
      get { return isolationLevel; }
    }

    /// <inheritdoc/>
    [DebuggerHidden]
    public override bool IsActive
    {
      get { return TransactionScopeBase<TScope, TTransaction>.CurrentTransaction == this; }
    }

    #region Commit, Rollback

    void ITransaction.Commit()
    {
      throw new NotSupportedException(
        Strings.ExScopeBoundTransactionCanBeCommittedOnlyByItsScope);
    }

    /// <summary>
    /// Commits the transaction.
    /// </summary>
    internal void Commit()
    {
      if (state != TransactionState.Active)
        throw new InvalidOperationException(String.Format(
          Strings.ExInvalidTransactionState, state, TransactionState.Active));
      state = TransactionState.Committing;
      try {
        OnCommit();
        state = TransactionState.Committed;
      }
      catch {
        state = TransactionState.RolledBack;
        throw;
      }
    }

    /// <summary>
    /// Rolls back the transaction.
    /// </summary>
    public void Rollback()
    {
      if (state != TransactionState.Active)
        throw new InvalidOperationException(String.Format(
          Strings.ExInvalidTransactionState, state, TransactionState.Active));
      state = TransactionState.RollingBack;
      try {
        OnRollback();
      }
      finally {
        state = TransactionState.RolledBack;
      }
    }

    #endregion

    /// <summary>
    /// Called when transaction is about to commit.
    /// </summary>
    protected abstract void OnCommit();

    /// <summary>
    /// Called when transaction is about to rollback.
    /// </summary>
    protected abstract void OnRollback();

    
    // Constructors
    
    /// <summary>
    /// <see cref="ClassDocTemplate.Ctor" copy="true" />
    /// </summary>
    /// <param name="identifier">The identifier.</param>
    protected TransactionBase(Guid identifier)
    {
      this.identifier = identifier;
    }
  }
}
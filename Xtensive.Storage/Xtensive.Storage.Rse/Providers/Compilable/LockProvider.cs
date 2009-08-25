// Copyright (C) 2009 Xtensive LLC.
// All rights reserved.
// For conditions of distribution and use, see license.
// Created by: Alexander Nikolaev
// Created:    2009.08.24

using System;
using Xtensive.Core.Internals.DocTemplates;

namespace Xtensive.Storage.Rse.Providers.Compilable
{
  /// <summary>
  /// Acquires the lock for a data in a source provider.
  /// </summary>
  [Serializable]
  public sealed class LockProvider : UnaryProvider
  {
    /// <summary>
    /// The mode of the lock to be acquired.
    /// </summary>
    public readonly LockMode LockMode;

    /// <summary>
    /// The behavior of the lock.
    /// </summary>
    public readonly LockBehavior LockBehavior;


    // Constructors

    /// <summary>
    /// <see cref="ClassDocTemplate.Ctor" copy="true"/>
    /// </summary>
    /// <param name="source">The <see cref="UnaryProvider.Source"/> property value.</param>
    /// <param name="lockMode">The mode of the lock to be acquired.</param>
    /// <param name="lockBehavior">The behavior of the lock.</param>
    public LockProvider(CompilableProvider source, LockMode lockMode, LockBehavior lockBehavior) :
      base(ProviderType.Lock, source)
    {
      LockMode = lockMode;
      LockBehavior = lockBehavior;
    }
  }
}
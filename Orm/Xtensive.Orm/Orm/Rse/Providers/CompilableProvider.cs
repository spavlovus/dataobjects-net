// Copyright (C) 2003-2010 Xtensive LLC.
// All rights reserved.
// For conditions of distribution and use, see license.
// Created by: Alexey Kochetov
// Created:    2008.07.03

using System;
using Xtensive.Orm.Tracing;

namespace Xtensive.Orm.Rse.Providers
{
  /// <summary>
  /// Abstract base class for any query provider,
  /// that requires storage-specific compilation before in can be executed.
  /// </summary>
  [Serializable]
  public abstract class CompilableProvider : Provider
  {
    /// <summary>
    /// Tracing information. It's supposed to be set only for root node in the providers tree.
    /// </summary>
    public TraceInfo TraceInfo { get; set; }

    // Constructors

    /// <inheritdoc/>
    protected CompilableProvider(ProviderType type, params Provider[] sources)
      : base(type, sources)
    {
    }
  }
}
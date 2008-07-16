// Copyright (C) 2008 Xtensive LLC.
// All rights reserved.
// For conditions of distribution and use, see license.
// Created by: Alexey Kochetov
// Created:    2008.07.09

using System;
using Xtensive.Core.Tuples;

namespace Xtensive.Storage.Rse.Providers
{
  [Serializable]
  public class RawProvider : CompilableProvider
  {
    private readonly RecordHeader header;
    public Tuple[] Tuples { get; private set; }

    protected override RecordHeader BuildHeader()
    {
      return header;
    }


    // Constructor

    public RawProvider(RecordHeader header, params Tuple[] tuples)
    {
      Tuples = tuples;
      this.header = header;
    }
  }
}
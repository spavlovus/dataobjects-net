// Copyright (C) 2009 Xtensive LLC.
// All rights reserved.
// For conditions of distribution and use, see license.
// Created by: Ivan Galkin
// Created:    2009.03.24

using System;
using System.Diagnostics;

namespace Xtensive.Indexing.Storage.Model
{
  /// <summary>
  /// A collection of <see cref="ValueColumnRef"/>.
  /// </summary>
  [Serializable]
  public class ValueColumnRefCollection: NodeCollectionBase<ValueColumnRef, IndexInfo>
  {
    // Constructors

    /// <inheritdoc/>
    public ValueColumnRefCollection(IndexInfo parent)
      : base(parent, "ValueColumns")
    {
    }
  }
}
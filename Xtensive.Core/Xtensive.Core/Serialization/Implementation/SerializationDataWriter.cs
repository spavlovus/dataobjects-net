﻿// Copyright (C) 2008 Xtensive LLC.
// All rights reserved.
// For conditions of distribution and use, see license.
// Created by: Dmitry Kononchuk
// Created:    2008.03.28

using System;
using Xtensive.Core.Internals.DocTemplates;

namespace Xtensive.Core.Serialization.Implementation
{
  /// <summary>
  /// Abstract base class for any record writer.
  /// </summary>
  public abstract class SerializationDataWriter : IDisposable
  {
    /// <summary>
    /// Creates a new <see cref="SerializationData"/> instance.
    /// </summary>
    /// <param name="reference">The <see cref="SerializationData.Reference"/> property value.</param>
    /// <param name="source">The <see cref="SerializationData.Source"/> property value.</param>
    /// <param name="origin">The <see cref="SerializationData.Origin"/> property value.</param>
    /// <returns>New <see cref="SerializationData"/> instance.</returns>
    public abstract SerializationData Create(IReference reference, object source, object origin, bool preferNesting);

    /// <summary>
    /// Appends (writes) the specified <paramref name="data"/> to the end of the stream.
    /// </summary>
    /// <param name="data">Record to append.</param>
    public abstract void Append(SerializationData data);

    /// <see cref="ClassDocTemplate.Dispose" copy="true"/>
    public abstract void Dispose();
  }
}
// Copyright (C) 2003-2010 Xtensive LLC.
// All rights reserved.
// For conditions of distribution and use, see license.
// Created by: Alex Yakunin
// Created:    2009.03.23

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using Xtensive.Core;

namespace Xtensive.Modelling.Actions
{
  /// <summary>
  /// Property change action.
  /// </summary>
  [Serializable]
  public class PropertyChangeAction : NodeAction
  {
    /// <summary>
    /// Gets or sets the properties.
    /// </summary>
    public IDictionary<string, object> Properties { get; private set; } = new Dictionary<string, object>(1, StringComparer.OrdinalIgnoreCase);

    /// <inheritdoc/>
    protected override void PerformExecute(IModel model, IPathNode item)
    {
      ArgumentValidator.EnsureArgumentNotNull(item, "item");
      var node = (Node) item;
      foreach (var pair in Properties)
        node.SetProperty(pair.Key, PathNodeReference.Resolve(model, pair.Value));
    }

    /// <inheritdoc/>
    protected override void GetParameters(List<Pair<string>> parameters)
    {
      base.GetParameters(parameters);
      foreach (var pair in Properties)
        parameters.Add(new Pair<string>(pair.Key, pair.Value==null ? null : pair.Value.ToString()));
    }

    /// <inheritdoc/>
    public override void Lock(bool recursive)
    {
      Properties = new ReadOnlyDictionary<string, object>(Properties);
      base.Lock(recursive);
    }
  }
}
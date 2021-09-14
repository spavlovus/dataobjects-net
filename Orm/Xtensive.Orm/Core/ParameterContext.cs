// Copyright (C) 2008-2020 Xtensive LLC.
// This code is distributed under MIT license terms.
// See the License.txt file in the project root for more information.
// Created by: Alex Kofman
// Created:    2008.08.14

using System;
using System.Collections.Generic;
using System.Diagnostics;
using Xtensive.Orm.Model;

namespace Xtensive.Core
{
  /// <summary>
  /// Provides storing context-specific <see cref="Parameter{TValue}"/>'s values.
  /// </summary>
  public sealed class ParameterContext
  {
    private readonly ParameterContext outerContext;

    private Dictionary<Parameter, object> values;
    private Dictionary<Parameter, object> Values => values ??= new Dictionary<Parameter, object>();

    private readonly TypeIdRegistry typeIdRegistry;

    [DebuggerStepThrough]
    internal bool TryGetValue(Parameter parameter, out object value)
    {
      value = default;
      return values?.TryGetValue(parameter, out value) == true || outerContext?.TryGetValue(parameter, out value) == true;
    }

    [DebuggerStepThrough]
    public TValue GetValue<TValue>(Parameter<TValue> parameter)
    {
      if (TryGetValue(parameter, out var result)) {
        return (TValue) result;
      }

      throw new InvalidOperationException(string.Format(Strings.ExValueForParameterXIsNotSet, parameter));
    }

    [DebuggerStepThrough]
    internal void SetValue(Parameter parameter, object value) => Values[parameter] = value;

    public int GetTypeId(TypeInfo type) =>
        typeIdRegistry?[type]
        ?? outerContext?.GetTypeId(type)
        ?? throw new InvalidOperationException(string.Format(Strings.ExTypeIdForTypeXIsNotFound, type));

    // Constructors

    /// <summary>
    /// Initializes new instance of this type.
    /// </summary>
    public ParameterContext(TypeIdRegistry typeIdRegistry = null, ParameterContext outerContext = null)
    {
      this.typeIdRegistry = typeIdRegistry;
      this.outerContext = outerContext;
    }
  }
}

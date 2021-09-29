// Copyright (C) 2003-2021 Xtensive LLC.
// This code is distributed under MIT license terms.
// See the License.txt file in the project root for more information.
// Created by: Denis Krjuchkov
// Created:    2009.11.07

using System;
using System.Collections.Generic;
using System.Globalization;
using Xtensive.Orm.Model;

namespace Xtensive.Sql.Compiler
{
  /// <summary>
  /// <see cref="PostCompiler"/> configuration.
  /// </summary>
  public sealed class SqlPostCompilerConfiguration
  {
    public HashSet<object> AlternativeBranches { get; } = new HashSet<object>();

    public Dictionary<object, string> PlaceholderValues { get; } = new Dictionary<object, string>();
    public TypeIdRegistry TypeIdRegistry { get; set; }

    public Dictionary<object, List<string[]>> DynamicFilterValues { get; } = new Dictionary<object, List<string[]>>();

    internal string GetPlaceholderValue(PlaceholderNode node) =>
      node.Id is TypeInfo typeInfo
          && (TypeIdRegistry?.GetTypeId(typeInfo) ?? TypeInfo.NoTypeId) is var typeId && typeId != TypeInfo.NoTypeId
        ? typeId.ToString(CultureInfo.InvariantCulture)
        : PlaceholderValues.TryGetValue(node.Id, out var value) ? value
        : throw new InvalidOperationException(string.Format(Strings.ExValueForPlaceholderXIsNotSet, node.Id));

    // Constructors

    public SqlPostCompilerConfiguration()
    {
    }
  }
}

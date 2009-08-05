// Copyright (C) 2009 Xtensive LLC.
// All rights reserved.
// For conditions of distribution and use, see license.
// Created by: Denis Krjuchkov
// Created:    2009.08.05

namespace Xtensive.Storage.Providers.Sql
{
  /// <summary>
  /// Possible types of <see cref="SqlFetchParameterBinding"/>.
  /// </summary>
  public enum SqlFetchParameterBindingType
  {
    /// <summary>
    /// Indicates no special handling of parameter values.
    /// </summary>
    Regular,
    /// <summary>
    /// Indicates that special handling of null values is performed.
    /// If <see cref="SqlFetchParameterBinding.ValueAccessor"/> returns <see langword="null"/>
    /// generated query with contain "something is null" check instead of "something = @p".
    /// </summary>
    SmartNull,
    /// <summary>
    /// Indicates that <see cref="bool"/> parameters is automatically propagated to constants
    /// according to a value returned by <see cref="SqlFetchParameterBinding.ValueAccessor"/>.
    /// </summary>
    BooleanConstant,
  }
}
// Copyright (C) 2008 Xtensive LLC.
// All rights reserved.
// For conditions of distribution and use, see license.
// Created by: Alexey Gamzov
// Created:    2008.05.26

using System;
using Xtensive.Storage.Model;
using Xtensive.Storage.Resources;
using Xtensive.Core.Tuples;

namespace Xtensive.Storage.Internals
{
  internal class DefaultFieldAccessor<T> : FieldAccessorBase<T>
  {
    private static readonly FieldAccessorBase<T> instance = new DefaultFieldAccessor<T>();
    private static readonly bool isObject = (typeof (T)==typeof (object));
    private static readonly bool isString = (typeof (T)==typeof (string));
    private static readonly bool isByteArray = (typeof (T)==typeof (byte[]));

    public static FieldAccessorBase<T> Instance {
      get { return instance; }
    }

    /// <inheritdoc/>
    public override T GetValue(Persistent obj, FieldInfo field, bool notify)
    {
      EnsureTypeIsAssignable(field);
      int fieldIndex = field.MappingInfo.Offset;
      var tuple = obj.Tuple;

      if (isObject)
        return (T) tuple.GetValueOrDefault(fieldIndex);

      return tuple.GetValueOrDefault<T>(fieldIndex);
    }

    /// <inheritdoc/>
    /// <exception cref="InvalidOperationException">Invalid arguments.</exception>
    public override void SetValue(Persistent obj, FieldInfo field, T value, bool notify)
    {
      if (!field.IsNullable && value==null)
        throw new InvalidOperationException(string.Format(
          Strings.ExNotNullableConstraintViolationOnFieldX, field));

      if (value!=null && field.Length > 0) {
        if (isString && field.Length < ((string) (object) value).Length)
          throw new InvalidOperationException(string.Format(
            Strings.ExLengthConstraintViolationOnFieldX, field));
        if (isByteArray && field.Length < ((byte[]) (object) value).Length)
          throw new InvalidOperationException(string.Format(
            Strings.ExLengthConstraintViolationOnFieldX, field));
      }

      EnsureTypeIsAssignable(field);
      obj.Tuple.SetValue(field.MappingInfo.Offset, value);
    }
  }
}
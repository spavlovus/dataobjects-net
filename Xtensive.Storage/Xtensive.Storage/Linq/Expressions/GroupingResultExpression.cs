// Copyright (C) 2009 Xtensive LLC.
// All rights reserved.
// For conditions of distribution and use, see license.
// Created by: Alexis Kochetov
// Created:    2009.04.06

using System;
using System.Linq.Expressions;
using Xtensive.Storage.Linq.Expressions.Mappings;
using Xtensive.Storage.Rse;

namespace Xtensive.Storage.Linq.Expressions
{
  internal sealed class GroupingResultExpression : ResultExpression
  {
    public ResultExpression Value { get; private set; }

    // Constructors

    public GroupingResultExpression(
      Type type, 
      RecordSet recordSet, 
      IMapping mapping, 
      Expression<Func<RecordSet, object>> projector, 
      ResultType resultType, 
      ResultExpression value)
      : base(type, recordSet, mapping, projector, resultType)
    {
      Value = value;
    }
  }
}
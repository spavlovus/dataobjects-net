// Copyright (C) 2008 Xtensive LLC.
// All rights reserved.
// For conditions of distribution and use, see license.
// Created by: Alexey Kochetov
// Created:    2008.12.02

using System.Linq;
using System.Linq.Expressions;
using Xtensive.Core.Linq;
using Xtensive.Core.Reflection;

namespace Xtensive.Storage.Linq
{
  internal static class ExpressionHelper
  {
    public static LambdaExpression StripQuotes(this Expression expression)
    {
      while (expression.NodeType == ExpressionType.Quote)
        expression = ((UnaryExpression)expression).Operand;
      return (LambdaExpression)expression;
    }

    public static bool IsQuery(this Expression expression)
    {
      return expression.Type.IsOfGenericType(typeof(IQueryable<>));
    }

    public static string ToSharpString(this Expression e)
    {
      return ExpressionWriter.WriteToString(e);
    }

    public static MemberType GetMemberType(this Expression e)
    {
      var type = e.Type;
      if (typeof(Key).IsAssignableFrom(type))
        return MemberType.Key;
      if (typeof(IEntity).IsAssignableFrom(type))
        return MemberType.Entity;
      if (typeof(Structure).IsAssignableFrom(type))
        return MemberType.Structure;
      if (typeof(EntitySetBase).IsAssignableFrom(type))
        return MemberType.EntitySet;
      return MemberType.Unknown;
    }
  }
}
// Copyright (C) 2003-2010 Xtensive LLC.
// All rights reserved.
// For conditions of distribution and use, see license.
// Created by: Denis Krjuchkov
// Created:    2009.11.06

using System.Collections.Generic;
using Xtensive.Core;

namespace Xtensive.Sql.Dml
{
  public class SqlDynamicFilter : SqlExpression
  {
    public object Id { get; private set; }

    public List<SqlExpression> Expressions { get; private set; }

    internal override SqlDynamicFilter Clone(SqlNodeCloneContext context) =>
      context.GetOrAdd(this, static (t, c) => {
        var clone = new SqlDynamicFilter(t.Id);
        foreach (var expression in t.Expressions) {
          clone.Expressions.Add(expression.Clone(c));
        }

        return clone;
      });

    public override void AcceptVisitor(ISqlVisitor visitor)
    {
      visitor.Visit(this); 
    }

    public override void ReplaceWith(SqlExpression expression)
    {
      ArgumentValidator.EnsureArgumentNotNull(expression, "expression");
      ArgumentValidator.EnsureArgumentIs<SqlDynamicFilter>(expression, "expression");
      var replacingExpression = (SqlDynamicFilter) expression;
      Id = replacingExpression.Id;
      Expressions.Clear();
      Expressions.AddRange(replacingExpression.Expressions);
    }


    // Constructors

    internal SqlDynamicFilter(object id)
      : base(SqlNodeType.DynamicFilter)
    {
      Id = id;
      Expressions = new List<SqlExpression>();
    }
  }
}
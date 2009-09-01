// Copyright (C) 2007 Xtensive LLC.
// All rights reserved.
// For conditions of distribution and use, see license.

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using Xtensive.Core;

namespace Xtensive.Sql.Dml
{
  [Serializable]
  public class SqlRow: SqlExpressionList
  {
    internal override object Clone(SqlNodeCloneContext context)
    {
      if (context.NodeMapping.ContainsKey(this))
        return context.NodeMapping[this];

      Collection<SqlExpression> expressionsClone = new Collection<SqlExpression>();
      foreach (SqlExpression e in expressions)
        expressionsClone.Add((SqlExpression)e.Clone(context));

      SqlRow clone = new SqlRow(expressionsClone);
      return clone;
    }


    public override void ReplaceWith(SqlExpression expression)
    {
      ArgumentValidator.EnsureArgumentNotNull(expression, "expression");
      ArgumentValidator.EnsureArgumentIs<SqlRow>(expression, "expression");
      SqlRow replacingExpression = expression as SqlRow;
      expressions.Clear();
      foreach (SqlExpression e in replacingExpression)
        expressions.Add(e);
    }

    internal SqlRow(IList<SqlExpression> expressions)
      : base(SqlNodeType.Row, expressions)
    {
      this.expressions = expressions;
    }

    public override void AcceptVisitor(ISqlVisitor visitor)
    {
      visitor.Visit(this);
    }
  }
}
// Copyright (C) 2003-2010 Xtensive LLC.
// All rights reserved.
// For conditions of distribution and use, see license.

using System;
using Xtensive.Core;

namespace Xtensive.Sql.Dml
{
  [Serializable]
  public class SqlVariable : SqlExpression, ISqlCursorFetchTarget
  {
    private string name;
    private readonly SqlValueType type;

    /// <summary>
    /// Gets the name.
    /// </summary>
    /// <value>The name.</value>
    public string Name
    {
      get { return name;}
    }

    /// <summary>
    /// Gets the type.
    /// </summary>
    /// <value>The type.</value>
    public SqlValueType Type
    {
      get { return type; }
    }

    public SqlDeclareVariable Declare()
    {
      return new SqlDeclareVariable(this);
    }

    public override void ReplaceWith(SqlExpression expression)
    {
      var replacingExpression = ArgumentValidator.EnsureArgumentIs<SqlVariable>(expression);
      name = replacingExpression.Name;
    }

    internal override SqlVariable Clone(SqlNodeCloneContext context) =>
      context.GetOrAdd(this, static (t, c) =>
        new SqlVariable(t.name, t.type));

    internal SqlVariable(string name, SqlValueType type)
      : base(SqlNodeType.Variable)
    {
      this.name = name;
      this.type = type;
    }

    public override void AcceptVisitor(ISqlVisitor visitor)
    {
      visitor.Visit(this);
    }
  }
}

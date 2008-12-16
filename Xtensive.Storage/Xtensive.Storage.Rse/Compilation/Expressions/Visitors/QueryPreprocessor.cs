// Copyright (C) 2008 Xtensive LLC.
// All rights reserved.
// For conditions of distribution and use, see license.
// Created by: Alexey Kochetov
// Created:    2008.11.26

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Xtensive.Core.Internals.DocTemplates;
using Xtensive.Core.Tuples;
using Xtensive.Storage.Model;
using FieldInfo=Xtensive.Storage.Model.FieldInfo;

namespace Xtensive.Storage.Rse.Compilation.Expressions.Visitors
{
  public sealed class QueryPreprocessor : ExpressionVisitor
  {
    #region Nested helper classes

    class MemberAccessChecker : ExpressionVisitor
    {
      private bool containsMemberAccess;

      public static bool ContainsMemberAccess(Expression expression)
      {
        var mac = new MemberAccessChecker();
        mac.Visit(expression);
        return mac.containsMemberAccess;
      }

      protected override Expression VisitMemberAccess(MemberExpression m)
      {
        containsMemberAccess = true;
        return base.VisitMemberAccess(m);
      }

      protected override Expression VisitUnknown(Expression expression)
      {
        return expression;
      }

      private MemberAccessChecker()
      {}
    }

    #endregion

    private readonly DomainModel model;
    private readonly HashSet<Expression> evaluationCandidates;

    public static Expression Translate(Expression expression, DomainModel model)
    {
      var candidates = EvaluationChecker.GetCandidates(expression);
      var queryProcessor = new QueryPreprocessor(model, candidates);
      expression = queryProcessor.Visit(expression);
      return expression;
    }

    protected override Expression Visit(Expression exp)
    {
      if (exp == null)
        return null;
      if (evaluationCandidates.Contains(exp)) {
        if (MemberAccessChecker.ContainsMemberAccess(exp))
          return ExtractParameter(exp);
        return Evaluate(exp);
      }
      return base.Visit(exp);
    }

    protected override Expression VisitConstant(ConstantExpression c)
    {
      if (c.Value == null)
        return c;
      var rootPoint = c.Value as IQueryable;
      if (rootPoint != null) {
        var type = model.Types[rootPoint.ElementType];
        var index = type.Indexes.PrimaryIndex;
        return new IndexAccessExpression(c.Type, index);
      }
      return base.VisitConstant(c);
    }

    protected override Expression VisitMethodCall(MethodCallExpression expression)
    {
      if (expression.Object != null && expression.Object.Type == typeof(Tuple)) {
        if (expression.Method.Name == "GetValue" || expression.Method.Name == "GetValueOrDefault") {
          var columnArgument = expression.Arguments[0];
          if (evaluationCandidates.Contains(columnArgument)) {
            int columnIndex;
            if (columnArgument.NodeType==ExpressionType.Constant)
              columnIndex = (int) ((ConstantExpression) columnArgument).Value;
            else {
              var columnFunc = Expression.Lambda<Func<int>>(columnArgument).Compile();
              columnIndex = columnFunc();
            }
            return Expression.Call(expression.Object, expression.Method, Expression.Constant(columnIndex));
          }
        }
      }
      return base.VisitMethodCall(expression);
    }

    protected override Expression VisitMemberAccess(MemberExpression m)
    {
      var memberNames = new Stack<string>();
      Expression e = m;
      while(e.NodeType == ExpressionType.MemberAccess) {
        var memberAccess = (MemberExpression)e;
        var member = (PropertyInfo)memberAccess.Member;
        TypeInfo type;
        if (model.Types.TryGetValue(member.PropertyType, out type) && !type.IsStructure) {
          memberNames.Push(memberAccess.Member.Name);
        }
        else {
          if (memberNames.Count > 0) {
            var name = memberNames.Pop();
            name = string.Format("{0}.{1}", memberAccess.Member.Name, name);
            memberNames.Push(name);
          }
          else
            memberNames.Push(memberAccess.Member.Name);
        }
        e = memberAccess.Expression;
      }
      if (e.NodeType == ExpressionType.Parameter) {
        var type = model.Types[e.Type];
        var fields = type.Fields;
        FieldInfo field = null;
        while(memberNames.Count > 0) {
          var name = memberNames.Pop();
          if (name == "Key")
            return m;
          field = fields[name];
          fields = field.Fields;
        }
        return new FieldAccessExpression(m.Type, field);
      }
      return base.VisitMemberAccess(m);
    }

    protected override Expression VisitBinary(BinaryExpression b)
    {
      Expression left = Visit(b.Left);
      Expression right = Visit(b.Right);
      int i = 0;
      return base.VisitBinary(b);
    }

    protected override Expression VisitUnknown(Expression expression)
    {
      return expression;
    }

    private static Expression ExtractParameter(Expression expression)
    {
      Type type = expression.Type;
      if (type.IsValueType)
        expression = Expression.Convert(expression, typeof(object));
      Expression<Func<object>> lambda = Expression.Lambda<Func<object>>(expression);
      return new ParameterAccessExpression(type, lambda);
    }

    private static Expression Evaluate(Expression e)
    {
      if (e.NodeType == ExpressionType.Constant)
        return e;
      Type type = e.Type;
      if (type.IsValueType)
        e = Expression.Convert(e, typeof(object));
      Expression<Func<object>> lambda = Expression.Lambda<Func<object>>(e);
      Func<object> fn = lambda.Compile();
      return Expression.Constant(fn(), type);
    }


    // Constructor

    /// <summary>
    ///   <see cref="ClassDocTemplate.Ctor" copy="true"/>
    /// </summary>
    private QueryPreprocessor(DomainModel model, HashSet<Expression> evaluationCandidates)
    {
      this.model = model;
      this.evaluationCandidates = evaluationCandidates;
    }
  }
}
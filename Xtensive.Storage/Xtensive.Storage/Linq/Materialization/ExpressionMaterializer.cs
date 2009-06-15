// Copyright (C) 2009 Xtensive LLC.
// All rights reserved.
// For conditions of distribution and use, see license.
// Created by: Alexey Gamzov
// Created:    2009.05.21

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Net.NetworkInformation;
using Xtensive.Core;
using Xtensive.Core.Linq;
using Xtensive.Core.Parameters;
using Xtensive.Core.Tuples;
using System.Linq;
using System.Reflection;
using Xtensive.Storage.Linq.Expressions;
using Xtensive.Storage.Linq.Expressions.Visitors;
using Xtensive.Storage.Linq.Rewriters;
using Xtensive.Core.Collections;
using Xtensive.Core.Reflection;

namespace Xtensive.Storage.Linq.Materialization
{
  [Serializable]
  internal class ExpressionMaterializer : PersistentExpressionVisitor
  {
    private static readonly MethodInfo BuildPersistentTupleMethodInfo;
    private static readonly MethodInfo GetTupleSegmentMethodInfo;
    private readonly TranslatorContext context;
    private readonly ParameterExpression tupleParameter;
    private readonly ParameterExpression itemMaterializationContextParameter;
    private readonly Dictionary<IEntityExpression, int> entityRegistry = new Dictionary<IEntityExpression, int>();
    private readonly HashSet<Parameter<Tuple>> tupleParameters;

    #region Public static methods

    public static LambdaExpression MakeLambda(Expression expression, TranslatorContext context, IEnumerable<Parameter<Tuple>> tupleParameters)
    {
      var tupleParameter = Expression.Parameter(typeof (Tuple), "tuple");
      var visitor = new ExpressionMaterializer(tupleParameter, context, null, tupleParameters);
      var processedExpression = OwnerRemover.RemoveOwner(expression);
      return FastExpression.Lambda(visitor.Visit(processedExpression), tupleParameter);
    }

    public static MaterializationInfo MakeMaterialization(ItemProjectorExpression projector, TranslatorContext context, IEnumerable<Parameter<Tuple>> tupleParameters)
    {
      var tupleParameter = Expression.Parameter(typeof (Tuple), "tuple");
      var materializationContextParameter = Expression.Parameter(typeof (ItemMaterializationContext), "mc");
      var visitor = new ExpressionMaterializer(tupleParameter, context, materializationContextParameter, tupleParameters);
      var lambda = FastExpression.Lambda(visitor.Visit(projector.Item), tupleParameter, materializationContextParameter);
      var count = visitor.entityRegistry.Count;
      return new MaterializationInfo(count, lambda);
    }

    #endregion

    #region Visitor methods overrsides

    protected override Expression VisitGroupingExpression(GroupingExpression groupingExpression)
    {
      // 1. Prepare subquery parameters.
      Parameter<Tuple> parameterOfTuple;
      Type elementType;
      ProjectionExpression projection;
      TranslatedQuery translatedQuery = PrepareSubqueryParameters(groupingExpression, out parameterOfTuple, out elementType, out projection);

      // 2. Create constructor
      var keyType = groupingExpression.KeyExpression.Type;
      var keyMaterializer = Visit(groupingExpression.KeyExpression);
      var groupingCtor = typeof (Grouping<,>)
        .MakeGenericType(keyType, elementType)
        .GetConstructor(new[] {typeof (ProjectionExpression), typeof (TranslatedQuery), typeof (Parameter<Tuple>), typeof (Tuple), keyType});

      // 3. Create result expression.
      var resultExpression = Expression.New(
        groupingCtor,
        Expression.Constant(projection),
        Expression.Constant(translatedQuery),
        Expression.Constant(parameterOfTuple),
        tupleParameter,
        keyMaterializer);

      // 4. Result must be IGrouping<,> instead of Grouping<,>. Convert result expression.
      return Expression.Convert(resultExpression, groupingExpression.Type);
    }

    protected override Expression VisitSubQueryExpression(SubQueryExpression subQueryExpression)
    {
      // 1. Prepare subquery parameters.
      Parameter<Tuple> parameterOfTuple;
      Type elementType;
      ProjectionExpression projection;
      TranslatedQuery translatedQuery = PrepareSubqueryParameters(subQueryExpression, out parameterOfTuple, out elementType, out projection);

      // 2. Create constructor
      var subQueryCtor = typeof (SubQuery<>)
        .MakeGenericType(elementType)
        .GetConstructor(new[] {typeof (ProjectionExpression), typeof (TranslatedQuery), typeof (Parameter<Tuple>), typeof (Tuple)});

      // 3. Create result expression.
      var resultExpression = Expression.New(
        subQueryCtor,
        Expression.Constant(projection),
        Expression.Constant(translatedQuery),
        Expression.Constant(parameterOfTuple),
        tupleParameter);

      return Expression.Convert(resultExpression, subQueryExpression.Type);
    }

    private TranslatedQuery PrepareSubqueryParameters(SubQueryExpression subQueryExpression, out Parameter<Tuple> parameterOfTuple, out Type elementType, out ProjectionExpression projection)
    {
      // 1. Rewrite recordset to parameter<tuple>
      var subqueryTupleParameter = context.GetTupleParameter(subQueryExpression.OuterParameter);
      var dataSource = ApplyParameterToTupleParameterRewriter.Rewrite(
        subQueryExpression.ProjectionExpression.ItemProjector.DataSource.Provider,
        subqueryTupleParameter,
        subQueryExpression.ApplyParameter)
        .Result;

      var itemProjector = new ItemProjectorExpression(subQueryExpression.ProjectionExpression.ItemProjector.Item, dataSource, subQueryExpression.ProjectionExpression.ItemProjector.Context);
      parameterOfTuple = context.GetTupleParameter(subQueryExpression.OuterParameter);

      // 2. Add only parameter<tuple>. Tuple value will be assigned 
      // at the moment of materialization in SubqQuery constructor
      var tupleParameterBindings = new Dictionary<Parameter<Tuple>, Tuple>(subQueryExpression
        .ProjectionExpression
        .TupleParameterBindings) {
          {parameterOfTuple, null}
        };

      projection = new ProjectionExpression(
        subQueryExpression.ProjectionExpression.Type,
        itemProjector,
        tupleParameterBindings, subQueryExpression.ProjectionExpression.ResultType);

      // 3. make translation 
      elementType = subQueryExpression.ProjectionExpression.ItemProjector.Item.Type;
      var resultType = Core.Reflection.SequenceHelper.GetSequenceType(elementType);
      var translateMethod = Translator.TranslateMethodInfo.MakeGenericMethod(new[] {resultType});
      return (TranslatedQuery) translateMethod.Invoke(context.Translator, new[] {projection});
    }

    protected override Expression VisitFieldExpression(FieldExpression expression)
    {
      var tupleExpression = GetTupleExpression(expression);

      // Materialize non-owned field.
      if (expression.Owner==null) {
        var tupleAccess = tupleExpression.MakeTupleAccess(expression.Field.ValueType, expression.Mapping.Offset);
        return tupleAccess;
      }

      return MaterializeThroughOwner(expression, tupleExpression);
    }

    protected override Expression VisitStructureExpression(StructureExpression expression)
    {
      var tupleExpression = GetTupleExpression(expression);

      // Materialize non-owned structure.
      if (expression.Owner==null) {
        var typeInfo = expression.PersistentType;
        var tuplePrototype = typeInfo.TuplePrototype;
        var mappingInfo = expression.Fields
          .OfType<FieldExpression>()
          .Where(f => f.ExtendedType==ExtendedExpressionType.Field)
          .OrderBy(f => f.Field.MappingInfo.Offset)
          .Select(f => new Pair<int>(f.Field.MappingInfo.Offset, f.Mapping.Offset))
          .Distinct()
          .ToArray();

        int[] columnMap = MaterializationHelper.GetColumnMap(tuplePrototype.Count, mappingInfo);

        var persistentTupleExpression = (Expression) Expression.Call(
          BuildPersistentTupleMethodInfo,
          tupleExpression,
          Expression.Constant(tuplePrototype),
          Expression.Constant(columnMap));
        return Expression.Convert(
          Expression.Call(
            WellKnownMembers.CreateStructure,
            Expression.Constant(expression.Type),
            persistentTupleExpression),
          expression.Type);
      }

      return MaterializeThroughOwner(expression, tupleExpression);
    }

    protected override Expression VisitKeyExpression(KeyExpression expression)
    {
      Expression tupleExpression = Expression.Call(
        GetTupleSegmentMethodInfo,
        GetTupleExpression(expression),
        Expression.Constant(expression.Mapping));
      return Expression.Call(
        WellKnownMembers.KeyCreate,
        Expression.Constant(expression.EntityType),
        tupleExpression,
        Expression.Constant(true));
    }

    protected override Expression VisitEntityExpression(EntityExpression expression)
    {
      var tupleExpression = GetTupleExpression(expression);
      return CreateEntity(expression, tupleExpression);
    }

    /// <exception cref="InvalidOperationException">Unable to materialize Entity.</exception>
    private Expression CreateEntity(IEntityExpression expression, Expression tupleExpression)
    {
      int index;
      if (!entityRegistry.TryGetValue(expression, out index)) {
        index = entityRegistry.Count;
        entityRegistry.Add(expression, index);
      }

      if (itemMaterializationContextParameter==null)
        throw new InvalidOperationException("Unable to materialize Entity.");

      var typeIdField = expression.Fields.SingleOrDefault(f => f.Name==WellKnown.TypeIdField);
      int typeIdIndex = typeIdField==null ? -1 : typeIdField.Mapping.Offset;

      var mappingInfo = expression.Fields
        .OfType<FieldExpression>()
        .Where(f => f.ExtendedType==ExtendedExpressionType.Field)
        .OrderBy(f => f.Field.MappingInfo.Offset)
        .Select(f => new Pair<int>(f.Field.MappingInfo.Offset, f.Mapping.Offset))
        .Distinct()
        .ToArray();

      var isMaterializedExpression = Expression.Call(
        itemMaterializationContextParameter,
        ItemMaterializationContext.IsMaterializedMethodInfo,
        Expression.Constant(index));
      var getEntityExpression = Expression.Call(
        itemMaterializationContextParameter,
        ItemMaterializationContext.GetEntityMethodInfo,
        Expression.Constant(index));
      var materializeEntityExpression = Expression.Call(
        itemMaterializationContextParameter,
        ItemMaterializationContext.MaterializeMethodInfo,
        Expression.Constant(index),
        Expression.Constant(typeIdIndex),
        Expression.Constant(expression.PersistentType),
        Expression.Constant(mappingInfo),
        tupleExpression);
      return Expression.TypeAs(
        Expression.Condition(
          isMaterializedExpression,
          getEntityExpression,
          materializeEntityExpression),
        expression.Type);
    }

    /// <exception cref="InvalidOperationException"><c>InvalidOperationException</c>.</exception>
    protected override Expression VisitEntityFieldExpression(EntityFieldExpression expression)
    {
      if (expression.Entity!=null)
        return Visit(expression.Entity);

      var tupleExpression = GetTupleExpression(expression);
      return CreateEntity(expression, tupleExpression);
    }

    protected override Expression VisitEntitySetExpression(EntitySetExpression expression)
    {
      var tupleExpression = GetTupleExpression(expression);
      return MaterializeThroughOwner(expression, tupleExpression);
    }

    protected override Expression VisitColumnExpression(ColumnExpression expression)
    {
      var tupleExpression = GetTupleExpression(expression);
      return tupleExpression.MakeTupleAccess(expression.Type, expression.Mapping.Offset);
    }

    protected override Expression VisitUnary(UnaryExpression u)
    {
      if (u.NodeType == ExpressionType.Convert && u.Type.IsNullable()) {
        var fieldExpression = u.Operand as FieldExpression;
        if (fieldExpression != null) {
          var tupleExpression = GetTupleExpression(fieldExpression);
          var tupleAccess = tupleExpression.MakeTupleAccess(u.Type, fieldExpression.Mapping.Offset);
          return tupleAccess;
        }
      }
      return base.VisitUnary(u);
    }

    #endregion

    #region Private Methods

    private Expression MaterializeThroughOwner(Expression target, Expression tuple)
    {
      return MaterializeThroughOwner(target, tuple, false);
    }


    private Expression MaterializeThroughOwner(Expression target, Expression tuple, bool defaultIfEmpty)
    {
      var field = target as FieldExpression;
      if (field!=null) {
        defaultIfEmpty |= field.DefaultIfEmpty;
        var owner = field.Owner;
        var materializedOwner = MaterializeThroughOwner((Expression) owner, tuple, defaultIfEmpty);
        if (defaultIfEmpty) {
          return Expression.Condition(
            Expression.Equal(materializedOwner, Expression.Constant(null, materializedOwner.Type)),
            Expression.Call(MaterializationHelper.GetDefaultMethodInfo.MakeGenericMethod(field.Type)),
            Expression.MakeMemberAccess(materializedOwner, field.Field.UnderlyingProperty));
        }
        return Expression.MakeMemberAccess(materializedOwner, field.Field.UnderlyingProperty);
      }
      return CreateEntity((EntityExpression) target, tuple);
    }

    private Expression GetTupleExpression(ParameterizedExpression expression)
    {
      if (expression.OuterParameter==null)
        return tupleParameter;

      var parameterOfTuple = context.GetTupleParameter(expression.OuterParameter);
      if (tupleParameters.Contains(parameterOfTuple)) {
        // Make access on Parameter<Tuple>
        return Expression.MakeMemberAccess(Expression.Constant(parameterOfTuple), WellKnownMembers.ParameterOfTupleValue);
      }

      // Use ApplyParameter for RecordSet predicates
      if (itemMaterializationContextParameter==null) {
        var projectionExpression = context.Bindings[expression.OuterParameter];
        var applyParameter = context.GetApplyParameter(projectionExpression);
        var applyParameterExpression = Expression.Constant(applyParameter);
        return Expression.Property(applyParameterExpression, WellKnownMembers.ApplyParameterValue);
      }

      return tupleParameter;
    }

    // ReSharper disable UnusedMember.Local
    private static Tuple BuildPersistentTuple(Tuple tuple, Tuple tuplePrototype, int[] mapping)
    {
      var result = tuplePrototype.CreateNew();
      tuple.CopyTo(result, mapping);
      return result;
    }

    private static Tuple GetTupleSegment(Tuple tuple, Segment<int> segment)
    {
      return tuple.GetSegment(segment).ToRegular();
    }

    // ReSharper restore UnusedMember.Local

    #endregion

    // Constructors

    private ExpressionMaterializer(ParameterExpression tupleParameter, TranslatorContext context, ParameterExpression itemMaterializationContextParameter, IEnumerable<Parameter<Tuple>> tupleParameters)
    {
      this.itemMaterializationContextParameter = itemMaterializationContextParameter;
      this.tupleParameter = tupleParameter;
      this.context = context;
      this.tupleParameters = new HashSet<Parameter<Tuple>>(tupleParameters);
    }

    static ExpressionMaterializer()
    {
      BuildPersistentTupleMethodInfo = typeof (ExpressionMaterializer).GetMethod("BuildPersistentTuple", BindingFlags.NonPublic | BindingFlags.Static);
      GetTupleSegmentMethodInfo = typeof (ExpressionMaterializer).GetMethod("GetTupleSegment", BindingFlags.NonPublic | BindingFlags.Static);
    }
  }
}
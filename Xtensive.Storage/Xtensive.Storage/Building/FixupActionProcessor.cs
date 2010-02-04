// Copyright (C) 2009 Xtensive LLC.
// All rights reserved.
// For conditions of distribution and use, see license.
// Created by: Dmitri Maximov
// Created:    2009.05.28

using System;
using System.Collections.Generic;
using System.Linq;
using Xtensive.Storage.Building.Builders;
using Xtensive.Storage.Building.Definitions;
using Xtensive.Storage.Building.DependencyGraph;
using Xtensive.Storage.Building.FixupActions;
using Xtensive.Storage.Model;
using Xtensive.Storage.Resources;

namespace Xtensive.Storage.Building
{
  [Serializable]
  internal static class FixupActionProcessor
  {
    public static void Run()
    {
      var context = BuildingContext.Current;

      using (Log.InfoRegion(Strings.LogProcessingFixupActions))
        while (context.ModelInspectionResult.Actions.Count > 0) {
          var action = context.ModelInspectionResult.Actions.Dequeue();
          Log.Info(string.Format(Strings.LogExecutingActionX, action));
          action.Run();
        }
    }

    public static void Process(AddTypeIdToKeyFieldsAction action)
    {
      action.Hierarchy.KeyFields.Add(new KeyField(WellKnown.TypeIdFieldName));
    }

    public static void Process(ReorderFieldsAction action)
    {
      var target = action.Target;
      var buffer = new List<FieldDef>(target.Fields.Count);

      foreach (var keyField in action.Hierarchy.KeyFields) {
        var fieldDef = target.Fields[keyField.Name];
        buffer.Add(fieldDef);
        target.Fields.Remove(fieldDef);
      }
      if (!action.Hierarchy.IncludeTypeId) {
        var typeIdField = target.Fields[WellKnown.TypeIdFieldName];
        buffer.Add(typeIdField);
        target.Fields.Remove(typeIdField);
      }
      buffer.AddRange(target.Fields);
      target.Fields.Clear();
      target.Fields.AddRange(buffer);
    }

    public static void Process(RemoveTypeAction action)
    {
      var context = BuildingContext.Current;
      context.DependencyGraph.RemoveNode(action.Type);
      context.ModelDef.Types.Remove(action.Type);
    }

    public static void Process(BuildGenericTypeInstancesAction action)
    {
      var context = BuildingContext.Current;

      // Making a copy of already built hierarchy set to avoid recursiveness
      var hierarchies = context.ModelDef.Hierarchies.ToList();
      var parameters = action.Type.UnderlyingType.GetGenericArguments();

      // We can produce generic instance types with exactly 1 parameter, e.g. EntityWrapper<TEntity> where TEntity : Entity
      if (parameters.Length!=1)
        throw new DomainBuilderException(string.Format(
          Strings.ExUnableToBuildGenericInstanceTypesForXTypeBecauseItContainsMoreThen1GenericParameter, action.Type.Name));

      var parameter = parameters[0];

      // Parameter must be constrained
      var constraints = parameter.GetGenericParameterConstraints();
      if (constraints.Length==0)
        throw new DomainBuilderException(string.Format(
          Strings.ExUnableToBuildGenericInstanceTypesForXTypeBecauseParameterIsNotConstrained, action.Type.Name));

      // Building instances for all hierarchies
      foreach (var hierarchy in hierarchies) {
        foreach (var constraint in constraints) {
          if (!constraint.IsAssignableFrom(hierarchy.Root.UnderlyingType))
            goto next;
        }
        Type instanceType = action.Type.UnderlyingType.MakeGenericType(hierarchy.Root.UnderlyingType);
        ModelDefBuilder.ProcessType(instanceType);

      next:
        continue;
      }
    }

    public static void Process(AddForeignKeyIndexAction action)
    {
      var type = action.Type;
      Func<IndexDef, bool> predicate = 
        i => i.IsSecondary && 
        i.KeyFields.Count==1 && 
        i.KeyFields[0].Key==action.Field.Name;
      if (type.Indexes.Any(predicate))
        return;
      var context = BuildingContext.Current;
      var queue = new Queue<TypeDef>();
      var interfaces = new HashSet<TypeDef>();
      queue.Enqueue(type);
      while (queue.Count > 0) {
        var item = queue.Dequeue();
        foreach (var @interface in context.ModelDef.Types.FindInterfaces(item.UnderlyingType)) {
          queue.Enqueue(@interface);
          interfaces.Add(@interface);
        }
      }
      if (interfaces.SelectMany(i => i.Indexes).Any(predicate))
        return;

      var attribute = new IndexAttribute(action.Field.Name);
      var indexDef = ModelDefBuilder.DefineIndex(type, attribute);
      type.Indexes.Add(indexDef);
    }

    public static void Process(AddPrimaryIndexAction action)
    {
      var type = action.Type;

      var primaryIndexes = type.Indexes.Where(i => i.IsPrimary).ToList();
      if (primaryIndexes.Count > 0)
        foreach (var primaryIndex in primaryIndexes)
          type.Indexes.Remove(primaryIndex);

      var generatedIndex = new IndexDef {IsPrimary = true};
      generatedIndex.Name = BuildingContext.Current.NameBuilder.BuildIndexName(type, generatedIndex);

      TypeDef hierarchyRoot;
      if (type.IsInterface) {
        var implementor = type.Implementors.First();
        hierarchyRoot = implementor;
      }
      else
        hierarchyRoot = type;

      var hierarchyDef = BuildingContext.Current.ModelDef.FindHierarchy(hierarchyRoot);

      foreach (KeyField pair in hierarchyDef.KeyFields)
        generatedIndex.KeyFields.Add(pair.Name, pair.Direction);

      // Check if user added secondary index equal to auto-generated primary index
      var userDefinedIndex = type.Indexes.Where(i => (i.KeyFields.Count==generatedIndex.KeyFields.Count) && i.KeyFields.SequenceEqual(generatedIndex.KeyFields)).FirstOrDefault();
      if (userDefinedIndex != null) {
        type.Indexes.Remove(userDefinedIndex);
        generatedIndex.FillFactor = userDefinedIndex.FillFactor;
        if (!string.IsNullOrEmpty(userDefinedIndex.MappingName))
          generatedIndex.MappingName = userDefinedIndex.MappingName;
      }
      type.Indexes.Add(generatedIndex);
    }

    public static void Process(MarkFieldAsSystemAction action)
    {
      action.Field.IsSystem = true;
      if (action.Type.IsEntity && action.Field.Name == WellKnown.TypeIdFieldName)
        action.Field.IsTypeId = true;
    }

    public static void Process(AddTypeIdFieldAction action)
    {
      FieldDef fieldDef = ModelDefBuilder.DefineField(typeof (Entity).GetProperty(WellKnown.TypeIdFieldName));
      fieldDef.IsTypeId = true;
      fieldDef.IsSystem = true;
      action.Type.Fields.Add(fieldDef);
    }

    public static void Process(MarkFieldAsNotNullableAction action)
    {
      action.Field.IsNullable = false;
    }

    public static void Process(CopyKeyFieldsAction action)
    {
      var target = action.Type;
      var source = action.Source;

      Action<FieldDef> createField = sourceFieldDef => {
        if (target.Fields.Contains(sourceFieldDef.Name)) 
          return;
        var fieldDef = target.DefineField(sourceFieldDef.Name, sourceFieldDef.ValueType);
        fieldDef.Attributes = sourceFieldDef.Attributes;
        if (!string.IsNullOrEmpty(sourceFieldDef.MappingName))
         fieldDef.MappingName = sourceFieldDef.MappingName;
      };

      foreach (var keyField in source.KeyFields) {
        var sourceFieldDef = source.Root.Fields[keyField.Name];
        createField(sourceFieldDef);
      }
      // copy system fields
      foreach (var sourceFieldDef in source.Root.Fields.Where(f => f.IsSystem)) {
        createField(sourceFieldDef);
      }
    }

    public static void Process(BuildImplementorListAction action)
    {
      var context = BuildingContext.Current;

      var node = context.DependencyGraph.TryGetNode(action.Type);
      node.Value.Implementors.AddRange(node.IncomingEdges.Where(e => e.Kind==EdgeKind.Implementation).Select(e => e.Tail.Value));
    }
  }
}
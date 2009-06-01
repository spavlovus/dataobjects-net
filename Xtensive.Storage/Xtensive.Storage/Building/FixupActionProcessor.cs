// Copyright (C) 2009 Xtensive LLC.
// All rights reserved.
// For conditions of distribution and use, see license.
// Created by: Dmitri Maximov
// Created:    2009.05.28

using System;
using System.Collections.Generic;
using System.Linq;
using Xtensive.Core;
using Xtensive.Core.Diagnostics;
using Xtensive.Storage.Building.Builders;
using Xtensive.Storage.Building.Definitions;
using Xtensive.Storage.Building.FixupActions;
using Xtensive.Storage.Model;

namespace Xtensive.Storage.Building
{
  [Serializable]
  internal static class FixupActionProcessor
  {
    public static void Run()
    {
      var context = BuildingContext.Current;

      using (Log.InfoRegion("Processing fixup actions"))
        while (context.ModelInspectionResult.Actions.Count > 0) {
          var action = context.ModelInspectionResult.Actions.Dequeue();
          Log.Info(string.Format("Executing action: '{0}'", action));
          action.Run();
        }
    }

    public static void Process(TypeIdAsKeyFieldAction action)
    {
      var context = BuildingContext.Current;
      action.Hierarchy.KeyFields.Add(new KeyField(WellKnown.TypeIdField));
    }

    public static void Process(ReorderFieldsAction action)
    {
      TypeDef root = action.Hierarchy.Root;
      var buffer = new List<FieldDef>(root.Fields.Count);

      foreach (var keyField in action.Hierarchy.KeyFields) {
        FieldDef fieldDef = root.Fields[keyField.Name];
        buffer.Add(fieldDef);
        root.Fields.Remove(fieldDef);
      }
      if (!action.Hierarchy.IncludeTypeId) {
        var typeIdField = root.Fields[WellKnown.TypeIdField];
        buffer.Add(typeIdField);
        root.Fields.Remove(typeIdField);
      }
      buffer.AddRange(root.Fields);
      root.Fields.Clear();
      root.Fields.AddRange(buffer);
    }

    public static void Process(RemoveTypeAction action)
    {
      BuildingContext.Current.ModelDef.Types.Remove(action.Type);
    }

    public static void Process(BuildGenericInstancesAction action)
    {
      var context = BuildingContext.Current;

      // Making a copy of already built hierarchy set to avoid recursiveness
      var hierarchies = context.ModelDef.Hierarchies.ToList();
      var parameters = action.Type.UnderlyingType.GetGenericArguments();

      // We can produce generic instance types with exactly 1 parameter, e.g. EntityWrapper<TEntity> where TEntity : Entity
      if (parameters.Length!=1)
        throw new DomainBuilderException(string.Format("Unable to build generic instance types for '{0}' type because it contains more then 1 generic parameter.", action.Type.Name));

      var parameter = parameters[0];

      // Parameter must be constrained
      var constraints = parameter.GetGenericParameterConstraints();
      if (constraints.Length==0)
        throw new DomainBuilderException(string.Format("Unable to build generic instance types for '{0}' type because parameter is not constrained.", action.Type.Name));

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

    public static void Process(AddIndexAction action)
    {
      var attribute = new IndexAttribute(action.Field.Name);
      var indexDef = ModelDefBuilder.DefineIndex(action.Type, attribute);
      action.Type.Indexes.Add(indexDef);
    }
  }
}
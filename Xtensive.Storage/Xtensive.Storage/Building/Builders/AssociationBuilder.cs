// Copyright (C) 2008 Xtensive LLC.
// All rights reserved.
// For conditions of distribution and use, see license.
// Created by: Dmitri Maximov
// Created:    2008.07.03

using System;
using System.Linq;
using Xtensive.Core;
using Xtensive.Core.Helpers;
using Xtensive.Storage.Building.Definitions;
using Xtensive.Storage.Model;
using Xtensive.Storage.Resources;

namespace Xtensive.Storage.Building.Builders
{
  internal static class AssociationBuilder
  {
    public static void BuildAssociation(FieldDef fieldDef, FieldInfo field)
    {
      BuildingContext context = BuildingContext.Current;
      TypeInfo referencedType = context.Model.Types[field.ValueType];
      Multiplicity m = field.IsEntitySet ? Multiplicity.ManyToZero : Multiplicity.OneToZero;
      var association = new AssociationInfo(field, referencedType, m, fieldDef.OnDelete);
      association.Name = context.NameBuilder.Build(association);
      // association.EntityType = EntitySetHelper.BuildReferenceType(association);
      context.Model.Associations.Add(association);

      if (!fieldDef.PairTo.IsNullOrEmpty())
        context.PairedAssociations.Add(new Pair<AssociationInfo, string>(association, fieldDef.PairTo));
    }

    public static void BuildAssociation(AssociationInfo origin, FieldInfo field)
    {
      BuildingContext context = BuildingContext.Current;
      var association = new AssociationInfo(field, origin.ReferencedType, origin.Multiplicity, origin.OnDelete);
      association.Name = context.NameBuilder.Build(association);
      // association.EntityType = EntitySetHelper.BuildReferenceType(association);
      context.Model.Associations.Add(association);

      Pair<AssociationInfo, string> pairTo = context.PairedAssociations.Where(p => p.First==origin).FirstOrDefault();
      if (pairTo.First!=null)
        context.PairedAssociations.Add(new Pair<AssociationInfo, string>(association, pairTo.Second));
    }

    public static void BuildPairedAssociation(AssociationInfo pairedAssociation, string masterFieldName)
    {
      FieldInfo masterField = pairedAssociation.ReferencedType.Fields.TryGetValue(masterFieldName);
      if (masterField==null)
        throw new DomainBuilderException(
          string.Format(Strings.ExPairedFieldXWasNotFoundInYType, masterFieldName, pairedAssociation.ReferencedType.Name));

      if (masterField.IsPrimitive || masterField.IsStructure)
        throw new DomainBuilderException(
          string.Format(Strings.PairedFieldXHasInsufficientTypeItShouldBeReferenceToEntityOrAEntitySet, masterFieldName));

      if (pairedAssociation.ReferencingField==masterField)
        throw new DomainBuilderException(
          string.Format(Strings.ReferencedFieldXAndPairedFieldAreEqual, pairedAssociation.ReferencingField.Name));

      FieldInfo pairedField = pairedAssociation.ReferencingField;
      AssociationInfo masterAssociation = masterField.Association;
      if (masterAssociation.PairTo!=null && masterAssociation.PairTo!=pairedAssociation)
        throw new InvalidOperationException(String.Format(Strings.ExMasterAssociationIsAlreadyPaired, masterAssociation.Name, masterAssociation.PairTo.Name));

      masterAssociation.PairTo = pairedAssociation;
      pairedAssociation.PairTo = masterAssociation;

      if (masterAssociation.IsMaster && pairedAssociation.IsMaster)
        masterAssociation.IsMaster = false;

      if (masterField.IsEntity) {
        if (pairedField.IsEntity) {
          masterAssociation.Multiplicity = Multiplicity.OneToOne;
          pairedAssociation.Multiplicity = Multiplicity.OneToOne;
        }
        if (pairedField.IsEntitySet) {
          masterAssociation.Multiplicity = Multiplicity.OneToMany;
          pairedAssociation.Multiplicity = Multiplicity.ManyToOne;
        }
      }

      if (masterField.IsEntitySet) {
        if (pairedField.IsEntity) {
          masterAssociation.Multiplicity = Multiplicity.ManyToOne;
          pairedAssociation.Multiplicity = Multiplicity.OneToMany;
        }
        if (pairedField.IsEntitySet) {
          masterAssociation.Multiplicity = Multiplicity.ManyToMany;
          pairedAssociation.Multiplicity = Multiplicity.ManyToMany;
        }
      }
    }
  }
}
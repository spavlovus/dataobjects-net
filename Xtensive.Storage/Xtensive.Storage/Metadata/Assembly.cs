// Copyright (C) 2008 Xtensive LLC.
// All rights reserved.
// For conditions of distribution and use, see license.
// Created by: Alex Kofman
// Created:    2008.12.24

using Xtensive.Core.Internals.DocTemplates;
using Xtensive.Storage.Attributes;
using Xtensive.Storage.Configuration;
using Xtensive.Core.Tuples;

namespace Xtensive.Storage.Metadata
{
  /// <summary>
  /// Persistent descriptor of an assembly with registered persistent types.
  /// Used for schema upgrade purposes.
  /// </summary>
  [SystemType(TypeId = 2)]
  [HierarchyRoot("AssemblyName", InheritanceSchema = InheritanceSchema.ClassTable)]
  public class Assembly : Entity
  {
    /// <summary>
    /// Gets or sets the name of the assembly.
    /// </summary>
    /// <value>The name of the assembly.</value>
    [Field(Length = 500)]
    public string AssemblyName { get; private set; }

    /// <summary>
    /// Gets or sets the assembly version.
    /// </summary>
    [Field(Length = 50)]
    public string Version { get; set; }

    /// <summary>
    /// <see cref="ClassDocTemplate.Ctor" copy="true"/>
    /// </summary>
    /// <param name="name">The assembly name.</param>
    public Assembly(string name) : base(Tuple.Create(name))
    {
    }
  }
}
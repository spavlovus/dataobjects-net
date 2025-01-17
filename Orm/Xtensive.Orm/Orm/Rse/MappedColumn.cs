// Copyright (C) 2003-2021 Xtensive LLC.
// This code is distributed under MIT license terms.
// See the License.txt file in the project root for more information.
// Created by: Alexey Kochetov
// Created:    2007.09.21

using System;
using Xtensive.Orm.Model;

namespace Xtensive.Orm.Rse
{
  /// <summary>
  /// Mapped column of the <see cref="RecordSetHeader"/>.
  /// </summary>
  [Serializable]
  public sealed class MappedColumn : Column
  {
    private const string ToStringFormat = "{0} = {1}";

    private readonly ColumnInfoRef columnInfo;
    /// <summary>
    /// Gets the reference that describes a column.
    /// </summary>
    public ref readonly ColumnInfoRef ColumnInfoRef => ref columnInfo;

    /// <inheritdoc/>
    public override string ToString()
    {
      return string.Format(ToStringFormat, base.ToString(), ColumnInfoRef);
    }

    /// <inheritdoc/>
    public override Column Clone(int newIndex)
    {
      return new MappedColumn(ColumnInfoRef, Name, newIndex, Type);
    }

    /// <inheritdoc/>
    public override Column Clone(string newName)
    {
      return new MappedColumn(this, newName);
    }

    // Constructors

    #region Basic constructors

    /// <summary>
    /// Initializes a new instance of this class.
    /// </summary>
    /// <param name="name"><see cref="Column.Name"/> property value.</param>
    /// <param name="index"><see cref="Column.Index"/> property value.</param>
    /// <param name="type"><see cref="Column.Type"/> property value.</param>
    public MappedColumn(string name, int index, Type type)
      : this(default, name, index, type)
    {
    }

    /// <summary>
    /// Initializes a new instance of this class.
    /// </summary>
    /// <param name="columnInfoRef"><see cref="ColumnInfoRef"/> property value.</param>
    /// <param name="index"><see cref="Column.Index"/> property value.</param>
    /// <param name="type"><see cref="Column.Type"/> property value.</param>
    public MappedColumn(in ColumnInfoRef columnInfoRef, int index, Type type)
      : this(columnInfoRef, columnInfoRef.ColumnName, index, type)
    {
    }

    /// <summary>
    /// Initializes a new instance of this class.
    /// </summary>
    /// <param name="columnInfoRef"><see cref="ColumnInfoRef"/> property value.</param>
    /// <param name="name"><see cref="Column.Name"/> property value.</param>
    /// <param name="index"><see cref="Column.Index"/> property value.</param>
    /// <param name="type"><see cref="Column.Type"/> property value.</param>
    public MappedColumn(in ColumnInfoRef columnInfoRef, string name, int index, Type type)
      : base(name, index, type, null)
    {
      columnInfo = columnInfoRef;
    }

    #endregion

    #region Clone constructors

    private MappedColumn(MappedColumn column, string newName)
      : base(newName, column.Index, column.Type, column)
    {
      columnInfo = column.ColumnInfoRef;
    }

    private MappedColumn(MappedColumn column, int newIndex)
      : base(column.Name, newIndex, column.Type, column)
    {
      columnInfo = column.ColumnInfoRef;
    }

    #endregion
  }
}
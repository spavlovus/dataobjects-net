// Copyright (C) 2007 Xtensive LLC.
// All rights reserved.
// For conditions of distribution and use, see license.
// Created by: Alex Yakunin
// Created:    2007.11.23

using System;
using System.Diagnostics;
using Xtensive.Core;

namespace Xtensive.Integrity.Atomicity
{
  /// <summary>
  /// Default implementation for <see cref="IRedoDescriptor"/>.
  /// Redo operation descriptor.
  /// </summary>
  [Serializable]
  public class RedoDescriptor: OperationDescriptor,
    IRedoDescriptor
  {
    private IUndoDescriptor oppositeDescriptor;
    private object[] arguments;
    private bool isUndone;

    [DebuggerHidden]
    public IUndoDescriptor OppositeDescriptor
    {
      get { return oppositeDescriptor; }
      set { oppositeDescriptor = value; }
    }

    [DebuggerHidden]
    public bool IsUndone
    {
      get { return isUndone; }
    }

    [DebuggerHidden]
    public object[] Arguments
    {
      get { return arguments; }
      set { arguments = value; }
    }

    public override void Invoke()
    {
      CallDescriptor.Method.Invoke(CallDescriptor.Target, arguments);
    }

    public override void Finalize(bool isUndone)
    {
      this.isUndone = isUndone;
      if (isUndone) {
        oppositeDescriptor = null;
      }
    }

    public override string ToString()
    {
      return String.Format("RedoDescriptor: {0}, {1} arguments", base.ToString(), arguments.Length);
    }

    #region IContext<RedoScope> Members

    public bool IsActive {
      get {
        return RedoScope.CurrentDescriptor==this;
      }
    }

    IDisposable IContext.Activate()
    {
      return Activate();
    }

    public RedoScope Activate()
    {
      throw new NotSupportedException();
    }

    #endregion
  }
}
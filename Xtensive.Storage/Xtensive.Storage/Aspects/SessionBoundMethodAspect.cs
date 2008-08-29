// Copyright (C) 2007 Xtensive LLC.
// All rights reserved.
// For conditions of distribution and use, see license.
// Created by: Nick Svetlov
// Created:    2008.06.25

using System;
using System.Diagnostics;
using System.Reflection;
using PostSharp.Extensibility;
using PostSharp.Laos;
using Xtensive.Core;
using Xtensive.Core.Aspects.Helpers;
using Xtensive.Core.Helpers;
using Xtensive.Core.Reflection;
using Xtensive.Storage.Attributes;

namespace Xtensive.Storage.Aspects
{
  [MulticastAttributeUsage(MulticastTargets.Method)]
  [AttributeUsage(AttributeTargets.Method, AllowMultiple = false, Inherited = false)]
  [Serializable]
  internal sealed class SessionBoundMethodAspect : OnMethodBoundaryAspect
  {
    public override bool CompileTimeValidate(MethodBase method)
    {
      if (!AspectHelper.ValidateContextBoundMethod<Session>(this, method))
        return false;
      return false;
      // Let's ignore the methods & properties with [Infrastructure] attribute
      var methodInfo = (MethodInfo) method;
      if (methodInfo.GetAttribute<InfrastructureAttribute>(
          AttributeSearchOptions.InheritFromAllBase |
          AttributeSearchOptions.InheritFromPropertyOrEvent)!=null)
        return false;

      return true;
    }

    [DebuggerStepThrough]
    public override void OnEntry(MethodExecutionEventArgs eventArgs)
    {
      var sessionBound = (SessionBound) eventArgs.Instance;
      var sessionScope = (SessionScope) sessionBound.ActivateContext();
      eventArgs.MethodExecutionTag = sessionScope;
    }

    [DebuggerStepThrough]
    public override void OnExit(MethodExecutionEventArgs eventArgs)
    {
      var d = (IDisposable) eventArgs.MethodExecutionTag;
      d.DisposeSafely();
    }
  }
}
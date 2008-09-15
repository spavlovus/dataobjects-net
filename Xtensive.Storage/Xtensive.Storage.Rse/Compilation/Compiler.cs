// Copyright (C) 2008 Xtensive LLC.
// All rights reserved.
// For conditions of distribution and use, see license.
// Created by: Alexey Kochetov
// Created:    2008.07.04

using System;
using System.Reflection;
using Xtensive.Core;
using Xtensive.Core.Collections;
using Xtensive.Core.Internals.DocTemplates;
using Xtensive.Core.Threading;
using Xtensive.Storage.Rse.Providers;
using System.Linq;

namespace Xtensive.Storage.Rse.Compilation
{
  /// <summary>
  /// Abstract base class for RSE <see cref="Provider"/> compilers.
  /// Provides <see cref="TypeCompiler"/>s for <see cref="CompilableProvider"/>s.
  /// </summary>
  public abstract class Compiler : AssociateProvider,
    ICompiler
  {
    private static readonly UrlInfo defaultLocation = new UrlInfo("rse://localhost/");
    private readonly ThreadSafeDictionary<Type, TypeCompiler> typeCompliers = 
      ThreadSafeDictionary<Type, TypeCompiler>.Create(new object());


    /// <summary>
    /// Gets the default location.
    /// </summary>
    public static UrlInfo DefaultLocation
    {
      get { return defaultLocation; }
    }

    #region ICompiler methods

    /// <inheritdoc/>
    public UrlInfo Location { get; private set; }

    /// <inheritdoc/>
    ExecutableProvider ICompiler.Compile(CompilableProvider provider, ExecutableProvider[] sources)
    {
      if (provider==null)
        return null;
      var c = GetCompiler(provider);
      if (c==null)
        return null;

      if (sources.Any(s => s == null))
        return null;
      var ep = c.Compile(provider, sources.ToArray());
      ep.Location = Location;
      return IsCompatible(ep) ? ep : ToCompatible(ep);
    }

    /// <inheritdoc/>
    public abstract bool IsCompatible(ExecutableProvider provider);

    /// <inheritdoc/>
    public abstract ExecutableProvider ToCompatible(ExecutableProvider provider);

    #endregion

    #region GetCompiler(...) methods (protected)

    /// <summary>
    /// Gets the compiler responsible for compilation of specified <paramref name="provider"/>.
    /// </summary>
    /// <param name="provider">Compilable provider to get the compiler for.</param>
    /// <returns>The compiler.</returns>
    protected TypeCompiler GetCompiler(Provider provider)
    {
      if (provider == null)
        return null;
      Type type = provider.GetType();
      return GetCompiler(type);
    }

    /// <summary>
    /// Gets the compiler responsible for compilation of provider of specified <paramref name="type"/>.
    /// </summary>
    /// <param name="type">The type of provider to get the compiler for.</param>
    /// <returns>The compiler.</returns>
    protected TypeCompiler GetCompiler(Type type)
    {
      return typeCompliers.GetValue(type,
        (_type, _this) => _this
          .GetType()
          .GetMethod("GetCompiler",
            BindingFlags.NonPublic | BindingFlags.Instance, null, ArrayUtils<Type>.EmptyArray, null)
          .GetGenericMethodDefinition()
          .MakeGenericMethod(new[] {_type})
          .Invoke(_this, null)
          as TypeCompiler,
        this);
    }

    /// <summary>
    /// Gets the compiler responsible for compilation of provider of specified <typeparamref name="TProvider"/>.
    /// </summary>
    /// <typeparam name="TProvider">The type of provider to get the compiler for.</typeparam>
    /// <returns>The compiler.</returns>
    protected TypeCompiler<TProvider> GetCompiler<TProvider>() 
      where TProvider : CompilableProvider
    {
      return GetAssociate<TProvider, TypeCompiler<TProvider>, TypeCompiler<TProvider>>();
    }


    #endregion

    #region Protected methods

    /// <inheritdoc/>
    protected sealed override TResult ConvertAssociate<TKey, TAssociate, TResult>(TAssociate associate)
    {
      if (ReferenceEquals(associate, null))
        return default(TResult);
      return (TResult)(object) associate;
    }

    #endregion


    // Constructor

    /// <summary>
    /// <see cref="ClassDocTemplate.Ctor" copy="true" />
    /// </summary>
    protected Compiler(UrlInfo location)
    {
      Location = location;
      TypeSuffixes = new[] { "Compiler" };
      Type t = GetType();
      Type baseType = typeof (Compiler);
      while(t != baseType && t != null) {
        AddHighPriorityLocation(t.Assembly, t.Namespace);
        t = t.BaseType;
      }
      AddHighPriorityLocation(baseType.Assembly, baseType.Namespace);
    }
  }
}
// Copyright (C) 2007 Xtensive LLC.
// All rights reserved.
// For conditions of distribution and use, see license.
// Created by: Dmitri Maximov
// Created:    2007.08.03

using System;
using System.Globalization;
using System.Reflection;
using Xtensive.Core;
using Xtensive.PluginManager;
using Xtensive.Storage.Building.Builders;
using Xtensive.Storage.Configuration;
using Xtensive.Storage.Model;
using Xtensive.Storage.Providers;
using Xtensive.Storage.Resources;

namespace Xtensive.Storage.Building
{
  /// <summary>
  /// Utility class for <see cref="Storage"/> building.
  /// </summary>
  public static class DomainBuilder
  {
    private static readonly PluginManager<ProviderAttribute> pluginManager =
      new PluginManager<ProviderAttribute>(typeof (HandlerFactory), Environment.CurrentDirectory);

    /// <summary>
    /// Builds the new <see cref="Domain"/> according to specified configuration.
    /// </summary>
    /// <param name="configuration">The storage configuration.</param>
    /// <returns>Newly created <see cref="Domain"/>.</returns>
    /// <exception cref="ArgumentNullException">When <paramref name="configuration"/> is null.</exception>
    /// <exception cref="DomainBuilderException">When at least one error have been occurred 
    /// during storage building process.</exception>
    public static Domain Build(DomainConfiguration configuration)
    {
      ArgumentValidator.EnsureArgumentNotNull(configuration, "configuration");

      if (!configuration.IsLocked)
        configuration.Lock(true);

      Validate(configuration);

      BuildingContext context = new BuildingContext(configuration);
      context.NameProvider = new NameProvider(configuration.NamingConvention);

      // TODO: AY: Fix immediately
      using (new BuildingScope(context)) {
        try {
          BuildModel();
          BuildDomain();
          BuildHandlerProvider();
          BuildGenerators();
        }
        catch (DomainBuilderException e) {
          context.RegisterError(e);
        }

        context.EnsureBuildSucceed();
      }
      return context.Domain;
    }

    private static void BuildModel()
    {
      ModelBuilder.Build();
    }

    private static void Validate(DomainConfiguration configuration)
    {
      Log.Info(Strings.LogValidatingConfiguration);
      if (configuration.Builders.Count > 0)
        foreach (Type type in configuration.Builders)
          ValidateBuilder(type);
    }

    private static void ValidateBuilder(Type type)
    {
      ArgumentValidator.EnsureArgumentNotNull(type, "type");

      ConstructorInfo constructor =
        type.GetConstructor(BindingFlags.Public | BindingFlags.Instance, null, new Type[0], null);

      if (constructor==null)
        throw new DomainBuilderException(
          string.Format(Strings.ExTypeXMustHavePublicInstanceParameterlessConstructorInOrderToBeUsedAsStorageDefinitionBuilder, type.FullName));

      if (!typeof (IDomainBuilder).IsAssignableFrom(type))
        throw new DomainBuilderException(
          string.Format(CultureInfo.CurrentCulture,
            Strings.ExTypeXDoesNotImplementYInterface, type.FullName, typeof (IDomainBuilder).FullName));
    }

    private static void BuildDomain()
    {
      Domain domain = new Domain();
      domain.HandlerAccessor = new HandlerAccessor(domain);
      domain.Configuration = BuildingScope.Context.Configuration;
      domain.Model = BuildingScope.Context.Model;
      domain.NameProvider = BuildingScope.Context.NameProvider;
      domain.KeyManager = new KeyManager(domain);
      BuildingScope.Context.Domain = domain;
    }

    private static void BuildGenerators()
    {
      Log.Info(Strings.LogBuildingKeyProviders);
      Registry<HierarchyInfo, DefaultGenerator> generators = BuildingScope.Context.Domain.KeyManager.Generators;
      foreach (HierarchyInfo hierarchy in BuildingScope.Context.Model.Hierarchies) {
        DefaultGenerator generator;
        if (hierarchy.Generator==typeof (DefaultGenerator))
          generator = BuildingScope.Context.Domain.HandlerAccessor.Factory.CreateHandler<DefaultGenerator>();
        else
          generator = (DefaultGenerator) Activator.CreateInstance(hierarchy.Generator);
        generator.Hierarchy = hierarchy;
        generator.Initialize();
        generators.Register(hierarchy, generator);
      }
      generators.Lock();
    }

    private static void BuildHandlerProvider()
    {
      Log.Info(Strings.LogBuildingHandlerProvider);
      HandlerAccessor handlerAccessor = BuildingScope.Context.Domain.HandlerAccessor;
      lock (pluginManager) {
        string protocol = handlerAccessor.Domain.Configuration.ConnectionInfo.Protocol;
        Type handlerProviderType = pluginManager[new ProviderAttribute(protocol)];

        if (handlerProviderType==null)
          throw new DomainBuilderException(
            string.Format(Strings.ExStorageProviderNotFound,
              protocol,
              Environment.CurrentDirectory));

        handlerAccessor.Factory = (HandlerFactory) Activator.CreateInstance(handlerProviderType);
        handlerAccessor.DomainHandler = handlerAccessor.Factory.CreateHandler<DomainHandler>();
        handlerAccessor.DomainHandler.Build();
      }
    }
  }
}
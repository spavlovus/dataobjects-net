// Copyright (C) 2007 Xtensive LLC.
// All rights reserved.
// For conditions of distribution and use, see license.
// Created by: Dmitri Maximov
// Created:    2007.08.03

using Xtensive.Storage.Building;
using Xtensive.Storage.Configuration;
using Xtensive.Storage.Model;
using Xtensive.Storage.Providers;

namespace Xtensive.Storage
{
  public sealed class Domain
  {
    /// <summary>
    /// Gets the domain model.
    /// </summary>
    public DomainInfo Model 
    {
      get { return ExecutionContext.Model; }
    }

    /// <summary>
    /// Gets the configuration.
    /// </summary>
    public DomainConfiguration Configuration
    {
      get { return ExecutionContext.Configuration; }
    }

    /// <summary>
    /// Gets the key manager.
    /// </summary>
    public KeyManager KeyManager
    {
      get { return ExecutionContext.KeyManager; }
    }

    /// <summary>
    /// Gets the execution context.
    /// </summary>
    internal ExecutionContext ExecutionContext { get; set; }

    /// <summary>
    /// Creates the session.
    /// </summary>
    /// <returns></returns>
    /// <returns>New <see cref="SessionScope"/> object.</returns>
    public SessionScope OpenSession()
    {
      return OpenSession(new SessionConfiguration());
    }

    /// <summary>
    /// Creates the session.
    /// </summary>
    /// <param name="configuration">The session configuration.</param>
    /// <returns>New <see cref="SessionScope"/> object.</returns>
    public SessionScope OpenSession(SessionConfiguration configuration)
    {
      SessionHandler handler = ExecutionContext.HandlerProvider.GetHandler<SessionHandler>();
      handler.ExecutionContext = ExecutionContext;
      handler.Session = new Session(ExecutionContext, handler, configuration);
      return new SessionScope(handler.Session);
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="SessionScope"/> class.
    /// </summary>
    /// <param name="userName">User name to authenticate.</param>
    /// <param name="authParams">Authentication parameters (e.g. password).</param>
    /// <returns>New <see cref="SessionScope"/> object.</returns>
    public SessionScope OpenSession(string userName, params object[] authParams)
    {
      return OpenSession(new SessionConfiguration(userName, authParams));
    }

    /// <summary>
    /// Builds the new <see cref="Domain"/> according to specified <see cref="DomainConfiguration"/>.
    /// </summary>
    /// <param name="configuration">The <see cref="DomainConfiguration"/>.</param>
    /// <returns>Newly built <see cref="Domain"/>.</returns>
    public static Domain Build(DomainConfiguration configuration)
    {
      return DomainBuilder.Build(configuration);
    }


    // Constructors

    internal Domain()
    {
    }
  }
}
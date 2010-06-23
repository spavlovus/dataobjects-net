// Copyright (C) 2003-2010 Xtensive LLC.
// All rights reserved.
// For conditions of distribution and use, see license.
// Created by: Dmitri Maximov
// Created:    2007.08.10

using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Xtensive.Core;
using Xtensive.Core.Caching;
using Xtensive.Core.Collections;
using Xtensive.Core.Diagnostics;
using Xtensive.Core.Disposing;
using Xtensive.Core.Internals.DocTemplates;
using Xtensive.Core.IoC;
using Xtensive.Integrity.Atomicity;
using Xtensive.Storage.Configuration;
using Xtensive.Storage.Disconnected;
using Xtensive.Storage.Internals;
using Xtensive.Storage.Operations;
using Xtensive.Storage.PairIntegrity;
using Xtensive.Storage.Providers;
using Xtensive.Storage.ReferentialIntegrity;
using Xtensive.Storage.Resources;
using EnumerationContext=Xtensive.Storage.Rse.Providers.EnumerationContext;

namespace Xtensive.Storage
{
  /// <summary>
  /// Data context, which all persistent objects are bound to.
  /// </summary>
  /// <remarks>
  /// <para>
  /// Each session has its own connection to database and set of materialized persistent instates.
  /// It contains identity map and tracks changes in bound persistent classes.
  /// </para>
  /// <para>
  /// <c>Session</c> implements <see cref="IContext"/> interface, it means that each <c>Session</c>
  /// can be either active or not active in particular thread (see <see cref="IsActive"/> property).
  /// Each thread can contain only one active session, it can be a accessed via 
  /// <see cref="Current">Session.Current</see> property or <see cref="Demand">Session.Demand()</see> method.
  /// </para>
  /// <para>
  /// Session can be open and activated by <see cref="Open(Xtensive.Storage.Domain)">Session.Open(domain)</see> method. 
  /// Existing session can be activated by <see cref="Activate"/> method.
  /// </para>
  /// </remarks>
  /// <example>
  /// <code lang="cs" source="..\Xtensive.Storage\Xtensive.Storage.Manual\DomainAndSession\DomainAndSessionSample.cs" region="Session sample"></code>
  /// </example>
  /// <seealso cref="Domain"/>
  /// <seealso cref="SessionBound" />
  [DebuggerDisplay("FullName = {FullName}")]
  public sealed partial class Session : DomainBound,
    IIdentified<long>,
    IContext<SessionScope>, 
    IDisposable,
    IHasExtensions
  {
    private const string IdentifierFormat = "#{0}";
    private const string FullNameFormat   = "{0}, #{1}";

    private static Func<Session> resolver;
    private static long lastUsedIdentifier;

    private readonly Pinner pinner = new Pinner();
    private SessionScope sessionScope;
    private ExtensionCollection extensions;
    private volatile bool isDisposed;

    /// <summary>
    /// Gets the configuration of the <see cref="Session"/>.
    /// </summary>
    public SessionConfiguration Configuration { get; private set; }

    /// <summary>
    /// Gets the name of the <see cref="Session"/>
    /// (useful mainly for debugging purposes - e.g. it is used in logs).
    /// </summary>
    public string Name { get; private set; }

    /// <summary>
    /// Gets the identifier of the session.
    /// Identifiers are unique in <see cref="AppDomain"/> scope.
    /// </summary>
    public long Identifier { get; private set; }

    /// <inheritdoc/>
    object IIdentified.Identifier { get { return Identifier; } }

    /// <summary>
    /// Gets the full name of the <see cref="Session"/>.
    /// Full name includes both <see cref="Name"/> and <see cref="Identifier"/>.
    /// </summary>
    public string FullName {
      get {
        string name = Name;
        return name.IsNullOrEmpty()
          ? string.Format(IdentifierFormat, Identifier)
          : string.Format(FullNameFormat, name, Identifier);
      }
    }

    /// <summary>
    /// Indicates whether debug event logging is enabled.
    /// Caches <see cref="Log.IsLogged"/> method result for <see cref="LogEventTypes.Debug"/> event.
    /// </summary>
    public bool IsDebugEventLoggingEnabled { get; private set; }

    /// <summary>
    /// Gets a value indicating whether <see cref="Persist"/> method is running.
    /// </summary>
    public bool IsPersisting { get; private set; }

    /// <summary>
    /// Gets or sets a value indicating whether only a system logic is enabled.
    /// </summary>
    public bool IsSystemLogicOnly { get; internal set; }

    /// <summary>
    /// Gets or sets a value indicating whether session is disconnected:
    /// a <see cref="DisconnectedState"/> is attached to it (not <see langword="null" />).
    /// </summary>
    public bool IsDisconnected { 
      get { return DisconnectedState!=null; } 
    }

    /// <summary>
    /// Gets the attached <see cref="Storage.DisconnectedState"/> object, if any.
    /// </summary>
    public DisconnectedState DisconnectedState { get; internal set; }

    /// <summary>
    /// Gets or sets timeout for all <see cref="IDbCommand"/>s that
    /// are executed within this session.
    /// <seealso cref="IDbCommand.CommandTimeout"/>
    /// </summary>
    public int? CommandTimeout { get; set; }

    /// <summary>
    /// Gets or sets the <see cref="Current"/> session resolver to use
    /// when there is no active <see cref="Session"/>.
    /// </summary>
    /// <remarks>
    /// The setter of this property can be invoked just once per application lifetime; 
    /// assigned resolver can not be changed.
    /// </remarks>
    /// <exception cref="NotSupportedException">Resolver is already assigned.</exception>
    public static Func<Session> Resolver {
      [DebuggerStepThrough]
      get {
        return resolver;
      }
      set {
        resolver = value;
        if (value==null)
          Rse.Compilation.CompilationContext.Resolver = null;
        else
          Rse.Compilation.CompilationContext.Resolver = () => {
            var session = resolver.Invoke();
            return session==null ? null : session.CompilationContext;
          };
      }
    }

    /// <summary>
    /// Gets the session service provider.
    /// </summary>
    public IServiceContainer Services { get; private set; }

    #region Private / internal members

    internal SessionHandler Handler { get; set; }

    internal HandlerAccessor Handlers { get; private set; }

    internal SyncManager PairSyncManager { get; private set; }

    internal RemovalProcessor RemovalProcessor { get; private set; }

    internal bool IsDelayedQueryRunning { get; private set; }

    internal CompilationContext CompilationContext { get { return Handlers.DomainHandler.CompilationContext; } }

    internal IOperationContext CurrentOperationContext { get; set; }

    internal IOperationContext BlockingOperationContext { get; private set; }

    private void EnsureNotDisposed()
    {
      if (isDisposed)
        throw new ObjectDisposedException(Strings.ExSessionIsAlreadyDisposed);
    }

    internal EnumerationContext CreateEnumerationContext()
    {
      Persist(PersistReason.Query);
      ProcessDelayedQueries(true);
      EnsureTransactionIsStarted();
      return Handler.CreateEnumerationContext();
    }

    #endregion

    #region IContext<...> methods

    /// <summary>
    /// Gets the current active <see cref="Session"/> instance.
    /// </summary>
    public static Session Current {
      [DebuggerStepThrough]
      get {
        return
          SessionScope.CurrentSession ?? (resolver==null ? null : resolver.Invoke());
      }
    }

    /// <summary>
    /// Gets the current <see cref="Session"/>, 
    /// or throws <see cref="InvalidOperationException"/>, 
    /// if active <see cref="Session"/> is not found.
    /// </summary>
    /// <returns>Current session.</returns>
    /// <exception cref="InvalidOperationException"><see cref="Session.Current"/> <see cref="Session"/> is <see langword="null" />.</exception>
    public static Session Demand()
    {
      var currentSession = Current;
      if (currentSession==null)
        throw new InvalidOperationException(Strings.ExActiveSessionIsRequiredForThisOperation);
      return currentSession;
    }

    /// <inheritdoc/>
    public bool IsActive { get { return Current==this; } }

    /// <inheritdoc/>
    public SessionScope Activate()
    {
      return SessionScope.CurrentSession==this
        ? null
        : new SessionScope(this);
    }


    /// <summary>
    /// Activates the session.
    /// </summary>
    /// <param name="throwIfAnotherSessionIsActive">If set to <see langword="true"/>, 
    /// <see cref="InvalidOperationException"/> is thrown if another session is active
    /// (performs session switching check).</param>
    /// <returns>A disposable object reverting the action.</returns>
    /// <exception cref="InvalidOperationException">Session switching is detected.</exception>
    public SessionScope Activate(bool throwIfAnotherSessionIsActive)
    {
      if (!throwIfAnotherSessionIsActive)
        return Activate();
      var currentSession = SessionScope.CurrentSession; // Not Session.Current -
      // to avoid possible comparison with Session provided by Session.Resolver.
      if (currentSession==null)
        return new SessionScope(this);
      else {
        if (currentSession!=this)
          throw new InvalidOperationException(
            Strings.ExAttemptToAutomaticallyActivateSessionXInsideSessionYIsBlocked
            .FormatWith(this, currentSession));
        // No activation is necessary here
        return null;
      }
    }

    /// <summary>
    /// Deactivates <see cref="Current"/> session making it equal to <see langword="null" />.
    /// </summary>
    /// <returns>A disposable object reverting the action.</returns>
    public static SessionScope Deactivate()
    {
      return SessionScope.CurrentSession==null
        ? null
        : new SessionScope(null);
    }

    /// <inheritdoc/>
    IDisposable IContext.Activate()
    {
      return Activate();
    }

    #endregion

    #region IHasExtensions Members

    /// <inheritdoc/>
    public IExtensionCollection Extensions {
      get {
        if (extensions==null)
          extensions = new ExtensionCollection();
        return extensions;
      }
    }

    #endregion

    /// <summary>
    /// Temporary overrides <see cref="CommandTimeout"/>.
    /// </summary>
    /// <param name="newTimeout">New <see cref="CommandTimeout"/> value.</param>
    /// <returns>Command timeout overriding scope.</returns>
    public IDisposable OverrideCommandTimeout(int? newTimeout)
    {
      var oldTimeout = CommandTimeout;
      CommandTimeout = newTimeout;
      return new Disposable(_ => { CommandTimeout = oldTimeout; });
    }

    /// <inheritdoc/>
    public override string ToString()
    {
      return FullName;
    }


    // Constructors

    internal Session(Domain domain, SessionConfiguration configuration, bool activate)
      : base(domain)
    {
      IsDebugEventLoggingEnabled = Log.IsLogged(LogEventTypes.Debug); // Just to cache this value

      // Both Domain and Configuration are valid references here;
      // Configuration is already locked
      Configuration = configuration;
      Name = configuration.Name;
      Identifier = Interlocked.Increment(ref lastUsedIdentifier);

      // Handlers
      Handlers = domain.Handlers;
      Handler = Handlers.HandlerFactory.CreateHandler<SessionHandler>();
      Handler.Session = this;
      Handler.Initialize();

      // Caches, registry
      EntityStateCache = CreateSessionCache(configuration);
      EntityChangeRegistry = new EntityChangeRegistry();

      // Etc...
      // AtomicityContext = new AtomicityContext(this, AtomicityContextOptions.Undoable);
      PairSyncManager = new SyncManager(this);
      RemovalProcessor = new RemovalProcessor(this);
      EntityEventBroker = new EntityEventBroker();
      CommandTimeout = configuration.DefaultCommandTimeout;
      if (activate)
        sessionScope = new SessionScope(this);
      BlockingOperationContext = new BlockingOperationContext(this);

      // Creating Services
      var serviceContainerType = Configuration.ServiceContainerType ?? typeof (ServiceContainer);
      Services = 
        ServiceContainer.Create(typeof (ServiceContainer), 
          from type in Domain.Configuration.Types.SessionServices
          from registration in ServiceRegistration.CreateAll(type)
          select registration,
          ServiceContainer.Create(serviceContainerType, Handler.CreateBaseServices()));
    }

    // IDisposable implementation

    /// <summary>
    /// <see cref="ClassDocTemplate.Dispose" copy="true"/>
    /// </summary>
    public void Dispose()
    {
      if (isDisposed)
        return;
      try {
        if (IsDebugEventLoggingEnabled)
          Log.Debug(Strings.LogSessionXDisposing, this);
        NotifyDisposing();
        Services.DisposeSafely();
        Handler.DisposeSafely();
        sessionScope.DisposeSafely();
        sessionScope = null;
      }
      finally {
        isDisposed = true;
      }
    }
  }
}

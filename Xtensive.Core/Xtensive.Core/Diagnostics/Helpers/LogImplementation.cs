// Copyright (C) 2007 Xtensive LLC.
// All rights reserved.
// For conditions of distribution and use, see license.
// Created by: Alex Yakunin
// Created:    2007.10.10

using System;
using System.Globalization;
using log4net.Util;
using Xtensive.Core.Diagnostics;
using Xtensive.Core.Helpers;
using Xtensive.Core.Internals.DocTemplates;
using Xtensive.Core.Resources;

namespace Xtensive.Core.Diagnostics.Helpers
{
  /// <summary>
  /// Actual <see cref="ILog"/> implementation
  /// forwarding all the events to its <see cref="RealLog"/>.
  /// </summary>
  public sealed class LogImplementation: ILog
  {
    private readonly string name;
    private readonly IRealLog realLog;
    private LogEventTypes loggedEventTypes;

    /// <inheritdoc/>
    public string Name
    {
      get { return name; }
    }

    /// <inheritdoc/>
    public string Text
    {
      get { return realLog.Text; }
    }

    /// <inheritdoc/>
    public IRealLog RealLog
    {
      get { return realLog; }
    }

    /// <inheritdoc/>
    public LogEventTypes LoggedEventTypes
    {
      get { return loggedEventTypes; }
    }

    /// <inheritdoc/>
    public bool IsLogged(LogEventTypes eventType)
    {
      LogCaptureScope currentScope = LogCaptureScope.CurrentScope;
      return ((loggedEventTypes | 
        (currentScope==null ? 0 : currentScope.CaptureEventTypes)) & eventType)!=0;
    }

    /// <inheritdoc/>
    public void UpdateCachedProperties()
    {
      loggedEventTypes = realLog.LoggedEventTypes;
    }

    #region ILog logging methods

    /// <inheritdoc/>
    public void Debug(string format, params object[] args)
    {
      RealLog.LogEvent(LogEventTypes.Debug, new SystemStringFormat(CultureInfo.InvariantCulture, format, args), null, RealLog, null);
    }

    /// <inheritdoc/>
    public Exception Debug(Exception exception, string format, params object[] args)
    {
      RealLog.LogEvent(LogEventTypes.Debug, new SystemStringFormat(CultureInfo.InvariantCulture, format, args), exception, RealLog, null);
      return exception;
    }

    /// <inheritdoc/>
    public Exception Debug(Exception exception)
    {
      return Debug(exception, Strings.LogException);
    }

    /// <inheritdoc/>
    public IDisposable DebugRegion(string format, params object[] args)
    {
      string title = string.Format(format, args);
      Debug(String.Format(Strings.LogRegionBegin, title));
      return new Disposable<IDisposable>(
        new LogIndentScope(),
        delegate(bool disposing, IDisposable disposable) {
          disposable.DisposeSafely();
          Debug(String.Format(Strings.LogRegionEnd, title));
        });
    }

    /// <inheritdoc/>
    public void Info(string format, params object[] args)
    {
      RealLog.LogEvent(LogEventTypes.Info, new SystemStringFormat(CultureInfo.InvariantCulture, format, args), null, RealLog, null);
    }

    /// <inheritdoc/>
    public Exception Info(Exception exception, string format, params object[] args)
    {
      RealLog.LogEvent(LogEventTypes.Info, new SystemStringFormat(CultureInfo.InvariantCulture, format , args), exception, RealLog, null);
      return exception;
    }

    /// <inheritdoc/>
    public Exception Info(Exception exception)
    {
      return Info(exception, Strings.LogException);
    }

    /// <inheritdoc/>
    public IDisposable InfoRegion(string format, params object[] args)
    {
      string title = string.Format(format, args);
      Info(String.Format(Strings.LogRegionBegin, title));
      return new Disposable<IDisposable>(
        new LogIndentScope(),
        delegate(bool disposing, IDisposable disposable) {
          disposable.DisposeSafely();
          Info(String.Format(Strings.LogRegionEnd, title));
        });
    }

    /// <inheritdoc/>
    public void Warning(string format, params object[] args)
    {
      RealLog.LogEvent(LogEventTypes.Warning, new SystemStringFormat(CultureInfo.InvariantCulture, format, args), null, RealLog, null);
    }

    public Exception Warning(Exception exception, string format, params object[] args)
    {
      RealLog.LogEvent(LogEventTypes.Warning, new SystemStringFormat(CultureInfo.InvariantCulture, format, args), exception, RealLog, null);
      return exception;
    }

    /// <inheritdoc/>
    public Exception Warning(Exception exception)
    {
      return Warning(exception, Strings.LogException);
    }

    /// <inheritdoc/>
    public void Error(string format, params object[] args)
    {
      RealLog.LogEvent(LogEventTypes.Error, new SystemStringFormat(CultureInfo.InvariantCulture, format, args), null, RealLog, null);
    }

    /// <inheritdoc/>
    public Exception Error(Exception exception, string format, params object[] args)
    {
      RealLog.LogEvent(LogEventTypes.Error, new SystemStringFormat(CultureInfo.InvariantCulture, format, args), exception, RealLog, null);
      return exception;
    }

    /// <inheritdoc/>
    public Exception Error(Exception exception)
    {
      return Error(exception, Strings.LogException);
    }

    /// <inheritdoc/>
    public void FatalError(string format, params object[] args)
    {
      RealLog.LogEvent(LogEventTypes.FatalError, new SystemStringFormat(CultureInfo.InvariantCulture, format, args), null, RealLog, null);
    }

    public Exception FatalError(Exception exception, string format, params object[] args)
    {
      RealLog.LogEvent(LogEventTypes.FatalError, new SystemStringFormat(CultureInfo.InvariantCulture, format, args), exception, RealLog, null);
      return exception;
    }

    /// <inheritdoc/>
    public Exception FatalError(Exception exception)
    {
      return FatalError(exception, Strings.LogException);
    }

    #endregion

    /// <inheritdoc/>
    public override string ToString()
    {
      return realLog.ToString();
    }

    #region IContext<LogCaptureScope> Members

    /// <inheritdoc/>
    bool IContext.IsActive {
      get {
        return false;
      }
    }

    /// <inheritdoc/>
    /// <exception cref="NotSupportedException">Always thrown by this method.</exception>
    IDisposable IContext.Activate()
    {
      return (this as IContext<LogCaptureScope>).Activate();
    }

    /// <inheritdoc/>
    /// <exception cref="NotSupportedException">Always thrown by this method.</exception>
    LogCaptureScope IContext<LogCaptureScope>.Activate()
    {
      throw new NotSupportedException(Strings.ExUseLogCaptureScopeConstructorInstead);
    }

    #endregion


    // Constructors

    /// <summary>
    /// <see cref="ClassDocTemplate.Ctor" copy="true" />
    /// </summary>
    /// <param name="realLog">Real log to wrap.</param>
    public LogImplementation(IRealLog realLog)
    {
      ArgumentValidator.EnsureArgumentNotNull(realLog, "realLog");
      this.realLog = realLog;
      this.name = realLog.Name;
      realLog.Log = this;
      UpdateCachedProperties();
    }
  }
}
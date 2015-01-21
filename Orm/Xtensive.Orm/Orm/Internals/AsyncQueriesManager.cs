﻿// Copyright (C) 2012 Xtensive LLC.
// All rights reserved.
// For conditions of distribution and use, see license.
// Created by: Alexey Kulakov
// Created:    2014.11.17

#if NET45
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xtensive.Orm.Providers;

namespace Xtensive.Orm.Internals
{
  internal class AsyncQueriesManager: SessionBound
  {
    private readonly Dictionary<StateLifetimeToken, Dictionary<Task, IncompletedTaskInfo>> asyncQueries;
    private readonly Dictionary<StateLifetimeToken, IList<Command>> blockingCommands;
    private readonly Dictionary<QueryTask, DelayedTask> queryToDelayedTaskMap; 
    private readonly object lockableObject = new object();

    internal long WorkingAsyncQueriesCount { get; private set; }

    internal long BlockingCommandsCount { get; private set; }

    /// <summary>
    /// Registers information about new asynchronous query
    /// </summary>
    /// <param name="lifetimeToken"></param>
    /// <param name="task"></param>
    /// <param name="cancellationTokenSource"></param>
    internal void AddNewAsyncQuery(StateLifetimeToken lifetimeToken, Task task, CancellationTokenSource cancellationTokenSource)
    {
      lock (lockableObject) {
        Dictionary<Task, IncompletedTaskInfo> tasks;
        if (asyncQueries.TryGetValue(lifetimeToken, out tasks)) {
          tasks.Add(task, new IncompletedTaskInfo(task, cancellationTokenSource));
          WorkingAsyncQueriesCount++;
        }
        else {
          tasks = new Dictionary<Task, IncompletedTaskInfo>();
          tasks.Add(task, new IncompletedTaskInfo(task, cancellationTokenSource));
          asyncQueries.Add(lifetimeToken, tasks);
          WorkingAsyncQueriesCount++;
        }
      }
    }

    /// <summary>
    /// Removes information about asynchronous query
    /// </summary>
    /// <param name="lifetimeToken"></param>
    /// <param name="task"></param>
    /// <returns></returns>
    internal bool TryRemoveFinishedAsyncQuery(StateLifetimeToken lifetimeToken, Task task)
    {
      lock (lockableObject) {
        Dictionary<Task, IncompletedTaskInfo> tasks;
        if (asyncQueries.TryGetValue(lifetimeToken, out tasks)) {
          var removingResult = tasks.Remove(task);
          if (removingResult)
            WorkingAsyncQueriesCount--;
          if (tasks.Count==0)
            asyncQueries.Remove(lifetimeToken);
          return removingResult;
        }
        return false;
      }
    }

    internal bool TryCancelAsyncQuery(StateLifetimeToken stateLifetimeToken, Task task)
    {
      lock (lockableObject) {
        Dictionary<Task, IncompletedTaskInfo> tasks;
        if (asyncQueries.TryGetValue(stateLifetimeToken, out tasks)) {
          IncompletedTaskInfo taskInfo;
          if (tasks.TryGetValue(task, out taskInfo)) {
            taskInfo.CancellationTokenSource.Cancel();
            if (tasks.Remove(task)) 
              WorkingAsyncQueriesCount--;
            if (tasks.Count==0)
              asyncQueries.Remove(stateLifetimeToken);
            return true;
          }
          return false;
        }
        return false;
      }
    }

    internal bool TryCancelAllAsyncQueriesForToken(StateLifetimeToken stateLifetimeToken)
    {
      lock (lockableObject) {
        Dictionary<Task, IncompletedTaskInfo> tasks;
        if (asyncQueries.TryGetValue(stateLifetimeToken, out tasks)) {
          foreach (var incompletedTaskInfo in tasks)
            incompletedTaskInfo.Value.CancellationTokenSource.Cancel(true);
          WorkingAsyncQueriesCount = WorkingAsyncQueriesCount - tasks.Count;
          return true;
        }
        return false;
      }
    }

    internal bool TryCancelAllAsyncQueries()
    {
      lock (lockableObject) {
        foreach (var asyncQuery in asyncQueries)
          foreach (var taskInfo in asyncQuery.Value)
            taskInfo.Value.CancellationTokenSource.Cancel();
        ClearAsyncQueryList();
        return true;
      }
    }

    internal bool HasAsyncQueries()
    {
      var result = asyncQueries.Count > 0;
      return result;
    }

    internal bool HasAsyncQueriesForToken(StateLifetimeToken stateLifetimeToken)
    {
      var @return = asyncQueries.ContainsKey(stateLifetimeToken);
      return @return;
    }

    internal void AddNewBlockingCommand(StateLifetimeToken token, Command command)
    {
      lock (lockableObject) {
        IList<Command> commands;
        if (blockingCommands.TryGetValue(token, out commands))
          commands.Add(command);
        else {
          var list = new List<Command> {command};
          blockingCommands.Add(token, list);
        }
      }
    }

    internal void DisposeBlockingCommands()
    {
      lock (lockableObject) {
        foreach (var blockingCommand in blockingCommands)
          foreach (var command in blockingCommand.Value)
            command.Dispose();
        ClearBlockingCommandList();
      }
    }

    internal void DisposeBlockingCommandForToken(StateLifetimeToken stateLifetimeToken)
    {
      lock (lockableObject) {
        IList<Command> commands;
        if (blockingCommands.TryGetValue(stateLifetimeToken, out commands)) {
          foreach (var command in commands)
            command.Dispose();
          blockingCommands.Remove(stateLifetimeToken);
        }
      }
    }

    internal bool HasBlockingCommandsForToken(StateLifetimeToken token)
    {
      lock (lockableObject) {
        IList<Command> commands;
        if (blockingCommands.TryGetValue(token, out commands))
          return commands.Any(el => !el.IsDisposed);
      }
      return false;
    }

    internal bool HasBlockingCommands()
    {
      lock (lockableObject) {
        foreach (var blockingCommand in blockingCommands)
          foreach (var command in blockingCommand.Value)
            if (!command.IsDisposed)
              return true;
      }
      return false;
    }

    /// <summary>
    /// Clear list of async queries and blocking commands.
    /// </summary>
    internal void ClearAsyncQueriesAndBlockingCommands()
    {
      ClearAsyncQueryList();
      ClearBlockingCommandList();
    }

    /// <summary>
    /// Register binding between <paramref name="delayedTask"/> and <paramref name="query"/>.
    /// </summary>
    /// <param name="query">Delayed query</param>
    /// <param name="delayedTask">Delayed task which created <paramref name="query"/></param>
    internal void AddNewDelayedTask(QueryTask query, DelayedTask delayedTask)
    {
      queryToDelayedTaskMap.Add(query, delayedTask);
    }

    /// <summary>
    /// Sets <see cref="DelayedTask"/> to started state for all <see cref="QueryTask"/>s from <paramref name="aliveQueries"/> which has bounded <see cref="DelayedTask"/> 
    /// and sets all died <see cref="DelayedTask"/>s to cancelled state.
    /// </summary>
    /// <param name="aliveQueries">Alive queries(i.e. which have alive <see cref="StateLifetimeToken"/>).</param>
    internal void SetDelayedTaskToStarted(IEnumerable<QueryTask> aliveQueries)
    {
      HashSet<QueryTask> startedQueries = new HashSet<QueryTask>();
      foreach (var aliveQuery in aliveQueries) {
        DelayedTask task;
        if (queryToDelayedTaskMap.TryGetValue(aliveQuery, out task)) {
          task.SetStarted();
          startedQueries.Add(aliveQuery);
        }
      }
      foreach (var query in queryToDelayedTaskMap.Keys.Where(query => !startedQueries.Contains(query))) {
        var task = queryToDelayedTaskMap[query];
        task.SetCanceled();
        queryToDelayedTaskMap.Remove(query);
      }
    }

    /// <summary>
    /// Sets <see cref="DelayedTask"/> to completed state for all <see cref="QueryTask"/>s from <paramref name="queries"/> which has bounded <see cref="DelayedTask"/>.
    /// </summary>
    /// <param name="queries">Finished queries.</param>
    internal void SetDelayedTasksToCompleted(IEnumerable<QueryTask> queries)
    {
      foreach (var finishedQuery in queries) {
        DelayedTask task;
        if (queryToDelayedTaskMap.TryGetValue(finishedQuery, out task)) {
          task.SetCompleted();
          queryToDelayedTaskMap.Remove(finishedQuery);
        }
      }
    }

    /// <summary>
    /// Sets <see cref="DelayedTask"/> to completed with exception state for all <see cref="QueryTask"/>s from <paramref name="queries"/> which has bounded <see cref="DelayedTask"/>.
    /// </summary>
    /// <param name="queries"><see cref="QueryTask"/>s which finished with exception.</param>
    /// <param name="exception">Exception which <paramref name="queries"/> finished with.</param>
    internal void SetDelayedTasksToFault(IEnumerable<QueryTask> queries, Exception exception)
    {
      foreach (var faultedQuery in queries) {
        DelayedTask task;
        if (queryToDelayedTaskMap.TryGetValue(faultedQuery, out task)) {
          task.SetFaulted(exception);
          queryToDelayedTaskMap.Remove(faultedQuery);
        }
      }
    }

    private void ClearAsyncQueryList()
    {
      asyncQueries.Clear();
      WorkingAsyncQueriesCount = 0;
    }

    private void ClearBlockingCommandList()
    {
      blockingCommands.Clear();
    }

    internal AsyncQueriesManager(Session session)
      : base(session)
    {
      blockingCommands = new Dictionary<StateLifetimeToken, IList<Command>>();
      asyncQueries = new Dictionary<StateLifetimeToken, Dictionary<Task, IncompletedTaskInfo>>();
      queryToDelayedTaskMap = new Dictionary<QueryTask, DelayedTask>();
      WorkingAsyncQueriesCount = 0;
    }
  }
}
#endif
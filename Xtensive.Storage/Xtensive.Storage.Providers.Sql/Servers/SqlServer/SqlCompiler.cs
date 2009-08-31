// Copyright (C) 2009 Xtensive LLC.
// All rights reserved.
// For conditions of distribution and use, see license.
// Created by: Vakhtina Elena
// Created:    2009.02.13

using System;
using System.Collections.Generic;
using Xtensive.Core;
using Xtensive.Core.Collections;
using Xtensive.Sql;
using Xtensive.Sql.Dml;
using Xtensive.Storage.Rse;
using Xtensive.Storage.Rse.Providers;
using Xtensive.Storage.Rse.Providers.Compilable;

namespace Xtensive.Storage.Providers.Sql.Servers.SqlServer
{
  internal class SqlCompiler : ManualPagingSqlCompiler
  {
    protected override ExecutableProvider VisitTake(TakeProvider provider)
    {
      var compiledSource = GetCompiled(provider.Source) as SqlProvider;
      if (compiledSource == null)
        return null;

      var query = ExtractSqlSelect(compiledSource);
      var count = provider.Count();
      if (query.Limit == 0 || query.Limit > count)
        query.Limit = count;
      if (!(provider.Source is TakeProvider) && !(provider.Source is SkipProvider))
        AddOrderByStatement(provider, query);
      return new SqlProvider(provider, query, Handlers, compiledSource);
    }

    protected override SqlExpression ProcessAggregate(SqlProvider source, List<SqlTableColumn> sourceColumns, AggregateColumn aggregateColumn)
    {
      var aggregateType = aggregateColumn.Type;
      var result = base.ProcessAggregate(source, sourceColumns, aggregateColumn);
      if (aggregateColumn.AggregateType == AggregateType.Avg) {
        var originType = source.Origin.Header.Columns[aggregateColumn.SourceIndex].Type;
        // floats are promoted to doubles, but we need the same type
        if (originType == aggregateType && originType != typeof (float))
          return result;
        var sqlType = Driver.BuildValueType(aggregateType, null, null, null);
        return SqlDml.Cast(SqlDml.Avg(SqlDml.Cast(sourceColumns[aggregateColumn.SourceIndex], sqlType)), sqlType);
      }
      // cast to decimal is dangerous, because 'decimal' defaults to integer type
      if (aggregateColumn.AggregateType == AggregateType.Sum && aggregateType != typeof(decimal))
        return SqlDml.Cast(result, Driver.BuildValueType(aggregateType, null, null, null));
      return result;
    }


    // Constructors

    public SqlCompiler(HandlerAccessor handlers, BindingCollection<object, ExecutableProvider> compiledSources)
      : base(handlers, compiledSources)
    {
    }
  }
}
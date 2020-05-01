using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using Xtensive.Orm.Tests.ObjectModel;
using Xtensive.Orm.Tests.ObjectModel.ChinookDO;
using Xtensive.Orm.Tracing;

namespace Xtensive.Orm.Tests.Linq
{
  public class TraceTest : ChinookDOModelTest
  {
    private IQueryable<Customer> CreateQuery1() => Session.Query.All<Customer>().Where(a => a.CustomerId > 0).Trace();

    private IQueryable<Customer> CreateQuery2() => Session.Query.All<Customer>().Trace();

    private IQueryable<Customer> CreateQuery3(QueryEndpoint qe) => qe.All<Customer>().Trace();

    private IQueryable<Customer> CreateQuery4(QueryEndpoint qe) => qe.All<Customer>().Trace();

    [Test]
    public void EnumerationProvidesTraceInfo()
    {
      var traceInfos = new List<TraceInfo>();
      Session.Events.DbCommandExecuting += (sender, args) => {
        traceInfos.AddRange(args.TraceInfos);
      };

      var subQuery = CreateQuery1();
      var query = CreateQuery2()
        .Where(c => c == Session.Query.Single<Customer>(subQuery.FirstOrDefault().Key));

      Assert.IsNotEmpty(query.ToArray());

      var caller = traceInfos.Single().Caller;
      Assert.AreEqual(nameof(CreateQuery2), caller.MemberName);
      Assert.IsNotEmpty(caller.FilePath);
      Assert.True(caller.LineNumber > 0);
    }

    [Test]
    public void CountProvidesTraceInfo()
    {
      var traceInfos = new List<TraceInfo>();
      Session.Events.DbCommandExecuting += (sender, args) => {
        traceInfos.AddRange(args.TraceInfos);
      };

      var count = CreateQuery2().Count();

      var caller = traceInfos.Single().Caller;
      Assert.AreEqual(nameof(CreateQuery2), caller.MemberName);
    }

    [Test]
    public void DelayedQueryProvidesTraceInfo()
    {
      var traceInfos = new List<TraceInfo>();
      Session.Events.DbCommandExecuting += (sender, args) => {
        traceInfos.AddRange(args.TraceInfos);
      };

      var query1 = Session.Query.ExecuteDelayed("a", q => CreateQuery3(q).Count());
      var query2 = Session.Query.ExecuteDelayed("b", q => CreateQuery4(q).Count());

      var result = query1.Value + query2.Value;

      CollectionAssert.AreEqual(new[] { nameof(CreateQuery3), nameof(CreateQuery4) },
        traceInfos.Select(i => i.Caller.MemberName));
    }
  }
}
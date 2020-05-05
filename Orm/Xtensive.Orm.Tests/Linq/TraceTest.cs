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
    public void TraceDoesNotAffectExpression()
    {
      string commandText = null;
      Session.Events.DbCommandExecuting += (sender, args) => {
        commandText = args.Command.CommandText;
      };

      var result =
        Session.Query.All<Customer>()
          .Trace()
          .Where(c => c.FirstName == "fn")
          .Select(c => c.FirstName)
          .ToArray();

      Assert.AreEqual(
        "SELECT [a].[FirstName] FROM [dbo].[Customer] [a] WHERE ([a].[FirstName] = N'fn');\r\n",
        commandText);
    }

    [Test]
    public void EnumerationProvidesTraceInfo()
    {
      var traces = new List<TraceInfo>();
      Session.Events.DbCommandExecuting += (sender, args) => {
        traces.AddRange(args.Traces);
      };

      var subQuery = CreateQuery1();
      var query = CreateQuery2()
        .Where(c => c == Session.Query.Single<Customer>(subQuery.FirstOrDefault().Key));

      Assert.IsNotEmpty(query.ToArray());

      var caller = traces.Single().Caller;
      Assert.AreEqual(nameof(CreateQuery2), caller.MemberName);
      Assert.IsNotEmpty(caller.FilePath);
      Assert.True(caller.LineNumber > 0);
    }

    [Test]
    public void CountProvidesTraceInfo()
    {
      var traces = new List<TraceInfo>();
      Session.Events.DbCommandExecuting += (sender, args) => {
        traces.AddRange(args.Traces);
      };

      var count = CreateQuery2().Count();

      var caller = traces.Single().Caller;
      Assert.AreEqual(nameof(CreateQuery2), caller.MemberName);
    }

    [Test]
    public void DelayedQueryProvidesTraceInfo()
    {
      var traces = new List<TraceInfo>();
      Session.Events.DbCommandExecuting += (sender, args) => {
        traces.AddRange(args.Traces);
      };

      var query1 = Session.Query.ExecuteDelayed("a", q => CreateQuery3(q).Count());
      var query2 = Session.Query.ExecuteDelayed("b", q => CreateQuery4(q).Count());

      var result = query1.Value + query2.Value;

      CollectionAssert.AreEqual(new[] { nameof(CreateQuery3), nameof(CreateQuery4) },
        traces.Select(i => i.Caller.MemberName));
    }
  }
}
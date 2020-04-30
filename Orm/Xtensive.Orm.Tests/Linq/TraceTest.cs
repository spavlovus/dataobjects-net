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

    [Test]
    public void EnumerationProvidesTraceInfo()
    {
      Caller caller = null;
      Session.Events.DbCommandExecuting += (sender, args) => {
        caller = args.TraceInfo.Caller;
      };

      var subQuery = CreateQuery1();
      var query = CreateQuery2()
        .Where(c => c == Session.Query.Single<Customer>(subQuery.FirstOrDefault().Key));

      Assert.IsNotEmpty(query.ToArray());
      Assert.AreEqual(nameof(CreateQuery2), caller.MemberName);
      Assert.IsNotEmpty(caller.FilePath);
      Assert.True(caller.LineNumber > 0);
    }

    [Test]
    public void CountProvidesTraceInfo()
    {
      Caller caller = null;
      Session.Events.DbCommandExecuting += (sender, args) => {
        caller = args.TraceInfo.Caller;
      };

      var count = CreateQuery2().Count();

      Assert.AreEqual(nameof(CreateQuery2), caller.MemberName);
    }
  }
}
﻿// Copyright (C) 2013 Xtensive LLC.
// All rights reserved.
// For conditions of distribution and use, see license.
// Created by: Denis Krjuchkov
// Created:    2013.02.14

using System;
using System.Linq;
using System.Linq.Expressions;
using NUnit.Framework;
using Xtensive.Orm.Tests.Issues.IssueJira0421_DateTimeAddXxxModel;

namespace Xtensive.Orm.Tests.Issues
{
  namespace IssueJira0421_DateTimeAddXxxModel
  {
    [HierarchyRoot]
    public class EntityWithDate : Entity
    {
      [Key, Field]
      public long Id { get; private set; }

      [Field]
      public DateTime Today { get; set; }
    }
  }

  [TestFixture]
  public class IssueJira0421_DateTimeAddXxx : AutoBuildTest
  {
    private DateTime today = DateTime.Today;

    protected override Configuration.DomainConfiguration BuildConfiguration()
    {
      var configuration = base.BuildConfiguration();
      configuration.Types.Register(typeof (EntityWithDate));
      return configuration;
    }

    protected override void PopulateData()
    {
      using (var session = Domain.OpenSession())
      using (var tx = session.OpenTransaction()) {
        var e = new EntityWithDate {Today = today};
        tx.Complete();
      }
    }

    private void RunAllTests(Func<double, Expression<Func<EntityWithDate, bool>>> filterProvider)
    {
      using (var session = Domain.OpenSession())
      using (session.OpenTransaction()) {
        RunTest(session, filterProvider.Invoke(1));
        RunTest(session, filterProvider.Invoke(20));
        RunTest(session, filterProvider.Invoke(-5));
        RunTest(session, filterProvider.Invoke(0));
      }
    }

    private static void RunTest(Session session, Expression<Func<EntityWithDate,bool>> filter)
    {
      var count = session.Query.All<EntityWithDate>().Count(filter);
      Assert.That(count, Is.EqualTo(1));
    }

    [Test]
    public void AddDaysTest()
    {
      RunAllTests(value => e => e.Today.AddDays(value)==today.AddDays(value));
    }

    [Test]
    public void AddHoursTest()
    {
      RunAllTests(value => e => e.Today.AddHours(value)==today.AddHours(value));
    }

    [Test]
    public void AddMinutesTest()
    {
      RunAllTests(value => e => e.Today.AddMinutes(value)==today.AddMinutes(value));
    }

    [Test]
    public void AddSecondsTest()
    {
      RunAllTests(value => e => e.Today.AddSeconds(value)==today.AddSeconds(value));
    }

    [Test]
    public void AddMillisecondsTest()
    {
      RunAllTests(value => e => e.Today.AddMilliseconds(value)==today.AddMilliseconds(value));
    }
  }
}
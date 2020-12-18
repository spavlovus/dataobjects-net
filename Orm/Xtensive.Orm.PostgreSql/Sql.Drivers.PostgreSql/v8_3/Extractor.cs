// Copyright (C) 2009-2020 Xtensive LLC.
// This code is distributed under MIT license terms.
// See the License.txt file in the project root for more information.

using System;
using System.Data.Common;
using Xtensive.Sql.Model;
using Xtensive.Sql.Dml;

namespace Xtensive.Sql.Drivers.PostgreSql.v8_3
{
  internal class Extractor : v8_2.Extractor
  {
    private const int indoptionDesc = 0x0001;

    protected override void AddSpecialIndexQueryColumns(SqlSelect query, SqlTableRef spc, SqlTableRef rel, SqlTableRef ind, SqlTableRef depend)
    {
      base.AddSpecialIndexQueryColumns(query, spc, rel, ind, depend);
      query.Columns.Add(ind["indoption"]);
    }

    protected override void ReadSpecialIndexProperties(DbDataReader dr, Index i)
    {
      base.ReadSpecialIndexProperties(dr, i);
      var indoption = (short[]) dr["indoption"];
      for (var j = 0; j < indoption.Length; j++) {
        int option = indoption[j];
        if ((option & indoptionDesc) == indoptionDesc) {
          i.Columns[j].Ascending = false;
        }
      }
    }

    // Consructors

    public Extractor(SqlDriver driver)
      : base(driver)
    {
    }
  }
}
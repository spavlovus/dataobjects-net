// Copyright (C) 2009-2022 Xtensive LLC.
// This code is distributed under MIT license terms.
// See the License.txt file in the project root for more information.

using System.Diagnostics;
using System.Text;
using Xtensive.Sql.Dml;
using Xtensive.Sql.Model;
using Xtensive.Sql.Compiler;

namespace Xtensive.Sql.Drivers.PostgreSql.v8_2
{
  internal class Translator : v8_1.Translator
  {
    /// <inheritdoc/>
    [DebuggerStepThrough]
    public override string QuoteString(string str) =>
      "E'" + str.Replace("'", "''").Replace(@"\", @"\\").Replace("\0", string.Empty) + "'";

    /// <inheritdoc/>
    public override void TranslateString(IOutput output, string str)
    {
      _ = output.Append('E');
      base.TranslateString(output, str);
    }

    /// <inheritdoc/>
    public override string Translate(SqlFunctionType type)
    {
      return type switch {
        //date
        SqlFunctionType.CurrentDate => "date_trunc('day', clock_timestamp())",
        SqlFunctionType.CurrentTimeStamp => "clock_timestamp()",
        _ => base.Translate(type),
      };
    }

    protected override void AppendIndexStorageParameters(IOutput output, Index index)
    {
      if (index.FillFactor != null) {
        _ = output.Append($"WITH(FILLFACTOR={index.FillFactor})");
      }
    }


    // Constructors

    public Translator(SqlDriver driver)
      : base(driver)
    {
    }
  }
}
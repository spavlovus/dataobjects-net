// Copyright (C) 2007 Xtensive LLC.
// All rights reserved.
// For conditions of distribution and use, see license.
// Created by: Nick Svetlov
// Created:    2007.11.28

using System;

namespace Xtensive.Core.Comparison
{
  [Serializable]
  internal sealed class Int16Comparer : ValueTypeComparer<short>
  {
    protected override IAdvancedComparer<short> CreateNew(ComparisonRules rules)
    {
      return new Int16Comparer(Provider, ComparisonRules.Combine(rules));
    }


    // Constructors

    public Int16Comparer(IComparerProvider provider, ComparisonRules comparisonRules)
      : base(provider, comparisonRules)
    {
      ValueRangeInfo = new ValueRangeInfo<short>(true, short.MinValue, true, short.MaxValue, true, 1);
    }
  }
}
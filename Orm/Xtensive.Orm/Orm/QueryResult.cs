// Copyright (C) 2020-2021 Xtensive LLC.
// This code is distributed under MIT license terms.
// See the License.txt file in the project root for more information.

using System;
using System.Collections;
using System.Collections.Generic;
using Xtensive.Orm.Linq.Materialization;

namespace Xtensive.Orm
{
  /// <summary>
  /// Represents result of sequence query execution.
  /// </summary>
  /// <typeparam name="TItem">The type of items in the sequence.</typeparam>
  public readonly struct QueryResult<TItem> : IEnumerable<TItem>
  {
    private class EnumerableReader : IMaterializingReader<TItem>
    {
      private readonly IEnumerable<TItem> items;

      public Session Session => null;

      public IEnumerator<TItem> AsEnumerator() => items.GetEnumerator();

      public IAsyncEnumerator<TItem> AsAsyncEnumerator() => throw new System.NotSupportedException();

      public EnumerableReader(IEnumerable<TItem> items)
      {
        this.items = items;
      }
    }

    private readonly StateLifetimeToken lifetimeToken;


    // DO NOT ENUMERATE this reader anywhere outside this class
    internal IMaterializingReader<TItem> Reader { get; }

    /// <inheritdoc/>
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    /// <inheritdoc/>
    public IEnumerator<TItem> GetEnumerator()
    {
      EnsureResultsAlive();
      return Reader.AsEnumerator();
    }

    /// <summary>
    /// Transforms <see cref="QueryResult{TItem}"/> to an <see cref="IAsyncEnumerable{T}"/> sequence.
    /// </summary>
    public async IAsyncEnumerable<TItem> AsAsyncEnumerable()
    {
      EnsureResultsAlive();
      var enumerator = Reader.AsAsyncEnumerator();
      while (await enumerator.MoveNextAsync().ConfigureAwait(false)) {
        yield return enumerator.Current;
      }
    }

    private void EnsureResultsAlive()
    {
      if (lifetimeToken != null && !lifetimeToken.IsActive) {
        throw new InvalidOperationException(Strings.ExThisInstanceIsExpiredDueToTransactionBoundaries);
      }
    }

    internal QueryResult(IMaterializingReader<TItem> reader, StateLifetimeToken lifetimeToken)
    {
      this.Reader = reader;
      this.lifetimeToken = lifetimeToken;
    }

    internal QueryResult(IEnumerable<TItem> items)
    {
      Reader = new EnumerableReader(items);
      this.lifetimeToken = default;
    }
  }
}
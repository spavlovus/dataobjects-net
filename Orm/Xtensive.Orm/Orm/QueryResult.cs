// Copyright (C) 2020 Xtensive LLC.
// This code is distributed under MIT license terms.
// See the License.txt file in the project root for more information.

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

      public IEnumerator<TItem> AsEnumerator() => items.GetEnumerator();

      public IAsyncEnumerator<TItem> AsAsyncEnumerator() => throw new System.NotSupportedException();

      public EnumerableReader(IEnumerable<TItem> items)
      {
        this.items = items;
      }
    }

    private readonly IMaterializingReader<TItem> reader;

    /// <inheritdoc/>
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    /// <inheritdoc/>
    public IEnumerator<TItem> GetEnumerator() => reader.AsEnumerator();

    /// <summary>
    /// Transforms <see cref="QueryResult{TItem}"/> to an <see cref="IAsyncEnumerable{T}"/> sequence.
    /// </summary>
    public async IAsyncEnumerable<TItem> AsAsyncEnumerable()
    {
      var enumerator = reader.AsAsyncEnumerator();
      while (await enumerator.MoveNextAsync().ConfigureAwait(false)) {
        yield return enumerator.Current;
      }
    }

    internal QueryResult(IMaterializingReader<TItem> reader)
    {
      this.reader = reader;
    }

    internal QueryResult(IEnumerable<TItem> items)
    {
      reader = new EnumerableReader(items);
    }
  }
}
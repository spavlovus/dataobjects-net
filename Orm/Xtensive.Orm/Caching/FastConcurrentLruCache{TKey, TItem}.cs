// Copyright (C) 2003-2010 Xtensive LLC.
// All rights reserved.
// For conditions of distribution and use, see license.
// Created by: Alex Ustinov
// Created:    2007.05.28

using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using BitFaster.Caching.Lru;
using Xtensive.Collections;
using Xtensive.Conversion;
using Xtensive.Core;


namespace Xtensive.Caching
{
  /// <summary>
  /// A set of items limited by the maximal amount of memory it can use, or by any other measure.
  /// Stores as many most frequently accessed items in memory as long as it is possible
  /// while maintaining the total size of cached items less or equal to <see cref="MaxSize"/>.
  /// </summary>
  /// <typeparam name="TKey">The key of the item.</typeparam>
  /// <typeparam name="TItem">The type of the item to cache.</typeparam>
  public class FastConcurrentLruCache<TKey, TItem> :
    CacheBase<TKey, TItem>
  {
    private FastConcurrentLru<TKey, TItem> imp;

    /// <inheritdoc/>
    public override int Count => imp.Count;

    /// <inheritdoc/>
    public long MaxSize { get; private set; }
    
    /// <inheritdoc/>
    public override void Clear() =>        //TODO: Change to imp.Clear() after updating BitFaster.Caching package to 1.0.4
      imp = new FastConcurrentLru<TKey, TItem>((int)MaxSize);

    /// <inheritdoc/>
    public override bool TryGetItem(TKey key, bool markAsHit, out TItem item) => imp.TryGet(key, out item);

    /// <inheritdoc/>
    public override bool ContainsKey(TKey key) => imp.TryGet(key, out var _);

    /// <inheritdoc/>
    public override TItem Add(TItem item, bool replaceIfExists)
    {
      var key = KeyExtractor(item);
      if (replaceIfExists) {
        imp.AddOrUpdate(key, item);
        return item;
      }
      else {
        return imp.GetOrAdd(key, _ => item);
      }
    }

    /// <inheritdoc/>
    public override void RemoveKey(TKey key) => imp.TryRemove(key);

    /// <inheritdoc/>
    public override void RemoveKey(TKey key, bool removeCompletely) => imp.TryRemove(key);

    /// <inheritdoc/>
    public override IEnumerator<TItem> GetEnumerator() => throw new NotImplementedException();

    public FastConcurrentLruCache(int maxSize, Converter<TItem, TKey> keyExtractor)
    {
      if (maxSize <= 0)
        ArgumentValidator.EnsureArgumentIsInRange(maxSize, 1, int.MaxValue, "maxSize");
      MaxSize = maxSize;
      KeyExtractor = keyExtractor;
      imp = new FastConcurrentLru<TKey, TItem>(maxSize);
    }
  }
}

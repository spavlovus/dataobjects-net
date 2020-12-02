using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Xtensive.Caching.Lru
{
  public interface IConcurrentDictionary<TKey, TValue>
  {
    bool TryGetValue(TKey key, out TValue item);
    bool TryAdd(TKey key, TValue newItem);
    bool TryRemove(TKey key, out TValue newItem);
  }

  public class NonBlockingConcurrentDictionary<TKey, TValue> : IConcurrentDictionary<TKey, TValue>
  {
    private readonly NonBlocking.ConcurrentDictionary<TKey, TValue> concurrentDictionaryImplementation;

    public NonBlockingConcurrentDictionary(int concurrencyLevel, int capacity, IEqualityComparer<TKey> comparer)
    {
      concurrentDictionaryImplementation =
        new NonBlocking.ConcurrentDictionary<TKey, TValue>(concurrencyLevel, capacity, comparer);
    }

    public NonBlockingConcurrentDictionary(int capacity)
    {
      concurrentDictionaryImplementation =
        new NonBlocking.ConcurrentDictionary<TKey, TValue>(capacity);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryGetValue(TKey key, out TValue item) => concurrentDictionaryImplementation.TryGetValue(key, out item);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryAdd(TKey key, TValue newItem) => concurrentDictionaryImplementation.TryAdd(key, newItem);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryRemove(TKey key, out TValue newItem) =>
      concurrentDictionaryImplementation.TryRemove(key, out newItem);
  }

  public class SystemConcurrentDictionary<TKey, TValue> : IConcurrentDictionary<TKey, TValue>
  {
    private readonly ConcurrentDictionary<TKey, TValue> concurrentDictionaryImplementation;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public SystemConcurrentDictionary(int concurrencyLevel, int capacity, IEqualityComparer<TKey> comparer)
    {
      concurrentDictionaryImplementation =
        new ConcurrentDictionary<TKey, TValue>(concurrencyLevel, capacity, comparer);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryGetValue(TKey key, out TValue item) => concurrentDictionaryImplementation.TryGetValue(key, out item);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryAdd(TKey key, TValue newItem) => concurrentDictionaryImplementation.TryAdd(key, newItem);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryRemove(TKey key, out TValue newItem) =>
      concurrentDictionaryImplementation.TryRemove(key, out newItem);
  }
}
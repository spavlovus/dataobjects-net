using System;
using System.Collections.Generic;
using Xtensive.Caching.Lru;

namespace BitFaster.Caching.Lru
{
    ///<inheritdoc/>
    public sealed class FastConcurrentLru<K, V> : TemplateConcurrentLru<K, V, LruItem<K, V>, LruPolicy<K, V>>
    {
        /// <summary>
        /// Initializes a new instance of the FastConcurrentLru class with the specified capacity that has the default 
        /// concurrency level, and uses the default comparer for the key type.
        /// </summary>
        /// <param name="capacity">The maximum number of elements that the FastConcurrentLru can contain.</param>
        /// <param name="dictFunc"></param>
        public FastConcurrentLru(int capacity,
            Func<int, int, IEqualityComparer<K>, IConcurrentDictionary<K, LruItem<K, V>>> dictFunc)
            : base(Defaults.ConcurrencyLevel, capacity, EqualityComparer<K>.Default, new LruPolicy<K, V>(), dictFunc)
        {
        }

        /// <summary>
        /// Initializes a new instance of the FastConcurrentLru class that has the specified concurrency level, has the 
        /// specified initial capacity, and uses the specified IEqualityComparer<T>.
        /// </summary>
        /// <param name="concurrencyLevel">The estimated number of threads that will update the FastConcurrentLru concurrently.</param>
        /// <param name="capacity">The maximum number of elements that the FastConcurrentLru can contain.</param>
        /// <param name="comparer">The IEqualityComparer<T> implementation to use when comparing keys.</param>
        /// <param name="dictFunc"></param>
        public FastConcurrentLru(int concurrencyLevel, int capacity, IEqualityComparer<K> comparer,
            Func<int, int, IEqualityComparer<K>, IConcurrentDictionary<K, LruItem<K, V>>> dictFunc)
            : base(concurrencyLevel, capacity, comparer, new LruPolicy<K, V>(), dictFunc)
        {
        }
    }
}

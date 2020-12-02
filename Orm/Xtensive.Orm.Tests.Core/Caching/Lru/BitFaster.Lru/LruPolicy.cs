using System.Runtime.CompilerServices;

namespace BitFaster.Caching.Lru
{
    /// <summary>
    /// Discards the least recently used items first. 
    /// </summary>
    public readonly struct LruPolicy<K, V> : IPolicy<K, V, LruItem<K, V>>
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public LruItem<K, V> CreateItem(K key, V value)
        {
            return new LruItem<K, V>(key, value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Touch(LruItem<K, V> item)
        {
            item.WasAccessed = true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ShouldDiscard(LruItem<K, V> item)
        {
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ItemDestination RouteHot(LruItem<K, V> item)
        {
            if (item.WasAccessed)
            {
                return ItemDestination.Warm;
            }

            return ItemDestination.Cold;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ItemDestination RouteWarm(LruItem<K, V> item)
        {
            if (item.WasAccessed)
            {
                return ItemDestination.Warm;
            }

            return ItemDestination.Cold;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ItemDestination RouteCold(LruItem<K, V> item)
        {
            if (item.WasAccessed)
            {
                return ItemDestination.Warm;
            }

            return ItemDestination.Remove;
        }
    }
}

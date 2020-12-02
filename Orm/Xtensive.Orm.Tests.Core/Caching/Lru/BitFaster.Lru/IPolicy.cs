namespace BitFaster.Caching.Lru
{
    public interface IPolicy<in K, in V, I> where I : LruItem<K, V>
    {
        I CreateItem(K key, V value);

        void Touch(I item);

        bool ShouldDiscard(I item);

        ItemDestination RouteHot(I item);

        ItemDestination RouteWarm(I item);

        ItemDestination RouteCold(I item);
    }
}

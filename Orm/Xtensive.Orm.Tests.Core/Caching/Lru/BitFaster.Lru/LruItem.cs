namespace BitFaster.Caching.Lru
{
    public class LruItem<K, V>
    {
        private volatile bool wasAccessed;
        private volatile bool wasRemoved;

        public LruItem(K k, V v)
        {
            Key = k;
            Value = v;
        }

        public readonly K Key;

        public readonly V Value;

        public bool WasAccessed
        {
            get => wasAccessed;
            set => wasAccessed = value;
        }

        public bool WasRemoved
        {
            get => wasRemoved;
            set => wasRemoved = value;
        }
    }
}

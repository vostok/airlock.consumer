using System;
using System.Collections.Generic;

namespace Vostok.AirlockConsumer.MetricsAggregator
{
    public class BucketKey : IEquatable<BucketKey>
    {
        public BucketKey(IReadOnlyDictionary<string, string> tags)
        {
            Tags = tags;
        }

        public IReadOnlyDictionary<string, string> Tags { get; }

        private readonly string key;

        public bool Equals(BucketKey other)
        {
            if (ReferenceEquals(null, other))
                return false;
            if (ReferenceEquals(this, other))
                return true;
            return string.Equals(key, other.key);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;
            if (ReferenceEquals(this, obj))
                return true;
            if (obj.GetType() != this.GetType())
                return false;
            return Equals((BucketKey) obj);
        }

        public override int GetHashCode()
        {
            return key != null ? key.GetHashCode() : 0;
        }
    }
}
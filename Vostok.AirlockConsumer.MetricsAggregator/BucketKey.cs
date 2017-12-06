using System;
using System.Collections.Generic;
using System.Linq;

namespace Vostok.AirlockConsumer.MetricsAggregator
{
    public class BucketKey : IEquatable<BucketKey>
    {
        private readonly string key;

        public BucketKey(IReadOnlyDictionary<string, string> tags)
        {
            Tags = tags;
            key = string.Join("|", tags.OrderBy(x => x.Key, StringComparer.OrdinalIgnoreCase).Select(x => $"{x.Key}={x.Value}"));
        }

        public IReadOnlyDictionary<string, string> Tags { get; }

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
            if (obj.GetType() != GetType())
                return false;
            return Equals((BucketKey)obj);
        }

        public override int GetHashCode()
        {
            return key != null ? key.GetHashCode() : 0;
        }
    }
}
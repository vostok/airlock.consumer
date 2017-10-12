using System;
using System.Collections.Generic;
using System.Linq;
using Vostok.Metrics;

namespace Vostok.AirlockConsumer.MetricsAggregator
{
    public class BucketKeyProvider : IBucketKeyProvider
    {
        private readonly HashSet<string> splittableTags;

        public BucketKeyProvider()
            : this(new []{ MetricsTagNames.Host, MetricsTagNames.Status, MetricsTagNames.Operation })
        {
        }

        public BucketKeyProvider(string[] splittableTags)
        {
            this.splittableTags = new HashSet<string>(splittableTags, StringComparer.OrdinalIgnoreCase);
        }

        public IEnumerable<BucketKey> GetBucketKeys(IReadOnlyDictionary<string, string> tags)
        {
            var keys = new List<Dictionary<string, string>> {new Dictionary<string, string>()};
            foreach (var kvp in tags)
            {
                var count = keys.Count;
                if (splittableTags.Contains(kvp.Key))
                {
                    for (var i = 0; i < count; i++)
                    {
                        keys.Add(new Dictionary<string, string>(keys[i])
                        {
                            { kvp.Key, "any" }
                        });
                    }
                }
                for (var i = 0; i < count; i++)
                    keys[i].Add(kvp.Key, kvp.Value);
            }
            return keys.Select(k => new BucketKey(k));
        }
    }
}
using System.Collections.Generic;

namespace Vostok.AirlockConsumer.MetricsAggregator
{
    internal interface IBucketKeyProvider
    {
        IEnumerable<BucketKey> GetBucketKeys(IReadOnlyDictionary<string, string> tags);
    }
}
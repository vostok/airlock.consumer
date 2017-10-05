using System.Collections.Generic;

namespace Vostok.AirlockConsumer.MetricsAggregator
{
    public interface IBucketKeyProvider
    {
        IEnumerable<BucketKey> GetBucketKeys(IReadOnlyDictionary<string, string> tags);
    }
}
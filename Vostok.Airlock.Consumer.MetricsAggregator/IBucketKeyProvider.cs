using System.Collections.Generic;

namespace Vostok.Airlock.Consumer.MetricsAggregator
{
    public interface IBucketKeyProvider
    {
        IEnumerable<BucketKey> GetBucketKeys(IReadOnlyDictionary<string, string> tags);
    }
}
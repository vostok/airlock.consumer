using System.Collections.Generic;

namespace Vstk.AirlockConsumer.MetricsAggregator
{
    public interface IBucketKeyProvider
    {
        IEnumerable<BucketKey> GetBucketKeys(IReadOnlyDictionary<string, string> tags);
    }
}
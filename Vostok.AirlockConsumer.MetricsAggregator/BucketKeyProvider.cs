using System;
using System.Collections.Generic;

namespace Vostok.AirlockConsumer.MetricsAggregator
{
    internal class BucketKeyProvider : IBucketKeyProvider
    {
        public IEnumerable<BucketKey> GetBucketKeys(IReadOnlyDictionary<string, string> tags)
        {
            throw new NotImplementedException();
        }
    }
}
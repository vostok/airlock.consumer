using System.Collections.Concurrent;
using System.Threading;
using Vostok.Airlock;
using Vostok.Commons.Extensions.UnitConvertions;
using Vostok.Metrics;

namespace Vostok.AirlockConsumer.MetricsAggregator
{
    internal class MetricAggregator : IMetricAggregator
    {
        private readonly IBucketKeyProvider bucketKeyProvider;
        private readonly IAirlock airlock;
        private readonly ConcurrentDictionary<string, ConcurrentDictionary<BucketKey, IBucket>> buckets;
        private Borders borders;

        public MetricAggregator(
            IBucketKeyProvider bucketKeyProvider,
            IAirlock airlock,
            Borders borders)
        {
            this.bucketKeyProvider = bucketKeyProvider;
            this.airlock = airlock;
            this.borders = borders;
            buckets = new ConcurrentDictionary<string, ConcurrentDictionary<BucketKey, IBucket>>();
        }

        public void ProcessMetricEvent(string routingKey, MetricEvent metricEvent)
        {
            var byRoutingKey = buckets.GetOrAdd(routingKey, _ => new ConcurrentDictionary<BucketKey, IBucket>());
            var bucketKeys = bucketKeyProvider.GetBucketKeys(metricEvent.Tags);
            var currentBorders = Interlocked.CompareExchange(ref borders, null, null);
            foreach (var bucketKey in bucketKeys)
            {
                var bucket = byRoutingKey.GetOrAdd(
                    bucketKey,
                    bk => new Bucket(bk.Tags, 1.Minutes(), currentBorders));
                bucket.Consume(metricEvent.Values, metricEvent.Timestamp);
            }
        }

        public void Reset(Borders nextBorders)
        {
            Interlocked.Exchange(ref borders, nextBorders);
            foreach (var byRoutingKey in buckets)
            {
                foreach (var bucket in byRoutingKey.Value)
                {
                    var metrics = bucket.Value.Reset(nextBorders);
                    foreach (var metricEvent in metrics)
                    {
                        airlock.Push(byRoutingKey.Key, metricEvent);
                    }
                }
            }
        }
    }
}
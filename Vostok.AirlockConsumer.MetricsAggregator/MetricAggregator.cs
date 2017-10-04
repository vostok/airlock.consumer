using System;
using System.Collections.Concurrent;
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

        public MetricAggregator(
            IBucketKeyProvider bucketKeyProvider,
            IAirlock airlock)
        {
            this.bucketKeyProvider = bucketKeyProvider;
            this.airlock = airlock;
            buckets = new ConcurrentDictionary<string, ConcurrentDictionary<BucketKey, IBucket>>();
        }

        public void ProcessMetricEvent(string routingKey, MetricEvent metricEvent)
        {
            var byRoutingKey = buckets.GetOrAdd(routingKey, _ => new ConcurrentDictionary<BucketKey, IBucket>());
            var bucketKeys = bucketKeyProvider.GetBucketKeys(metricEvent.Tags);
            foreach (var bucketKey in bucketKeys)
            {
                var bucket = byRoutingKey.GetOrAdd(
                    bucketKey,
                    bk => new Bucket(TODO, bk.Tags, 1.Minutes()));
                bucket.Consume(metricEvent.Values, metricEvent.Timestamp);
            }
        }

        public void Reset(DateTimeOffset timestamp)
        {
            foreach (var byRoutingKey in buckets)
            {
                foreach (var bucket in byRoutingKey.Value)
                {
                    var metrics = bucket.Value.Reset(timestamp);
                    foreach (var metricEvent in metrics)
                    {
                        airlock.Push(byRoutingKey.Key, metricEvent);
                    }
                }
            }
        }
    }
}
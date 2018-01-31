using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using Vostok.Airlock;
using Vostok.Commons.Extensions.UnitConvertions;
using Vostok.Metrics;

namespace Vostok.AirlockConsumer.MetricsAggregator
{
    public class MetricAggregator : IDisposable
    {
        private readonly AggregatorMetrics aggregatorMetrics;
        private readonly IBucketKeyProvider bucketKeyProvider;
        private readonly IMetricSender metricSender;
        private readonly TimeSpan cooldownPeriod;
        private readonly ConcurrentDictionary<BucketKey, IBucket> buckets;
        private readonly string metricsRoutingKey;
        private Borders borders;

        public MetricAggregator(
            IMetricScope metricScope,
            IBucketKeyProvider bucketKeyProvider,
            IMetricSender metricSender,
            TimeSpan cooldownPeriod,
            Borders borders,
            string eventsRoutingKey)
        {
            aggregatorMetrics = new AggregatorMetrics(metricScope.WithTags(new Dictionary<string, string>
            {
                {MetricsTagNames.Type, "aggregation"}, {"routingKey", eventsRoutingKey}
            }));
            this.bucketKeyProvider = bucketKeyProvider;
            this.metricSender = metricSender;
            this.cooldownPeriod = cooldownPeriod;
            this.borders = borders;
            metricsRoutingKey = RoutingKey.ReplaceSuffix(eventsRoutingKey, RoutingKey.MetricsSuffix);
            buckets = new ConcurrentDictionary<BucketKey, IBucket>();
        }

        public void ProcessMetricEvent(MetricEvent metricEvent)
        {
            var bucketKeys = bucketKeyProvider.GetBucketKeys(metricEvent.Tags);
            var currentBorders = Interlocked.CompareExchange(ref borders, null, null);
            foreach (var bucketKey in bucketKeys)
            {
                var bucket = buckets.GetOrAdd(
                    bucketKey,
                    bk => new Bucket(aggregatorMetrics, bk.Tags, 1.Minutes(), cooldownPeriod, currentBorders));
                bucket.Consume(metricEvent.Values, metricEvent.Timestamp);
            }
        }

        public void Flush(Borders nextBorders)
        {
            Interlocked.Exchange(ref borders, nextBorders);
            foreach (var bucket in buckets)
            {
                var metrics = bucket.Value.Flush(nextBorders);
                PushToAirlock(metrics);
            }
        }

        private void PushToAirlock(IEnumerable<MetricEvent> metrics)
        {
            foreach (var metricEvent in metrics)
                metricSender.SendMetric(metricsRoutingKey, metricEvent);
        }

        public void Dispose()
        {
            aggregatorMetrics.Dispose();
        }
    }
}
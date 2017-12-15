using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Vostok.Airlock;
using Vostok.Commons.Extensions.UnitConvertions;
using Vostok.Logging;
using Vostok.Metrics;

namespace Vostok.AirlockConsumer.MetricsAggregator
{
    public class MetricAggregator : IMetricAggregator, IDisposable
    {
        private readonly IBucketKeyProvider bucketKeyProvider;
        private readonly IAirlockClient airlockClient;
        private readonly TimeSpan cooldownPeriod;
        private readonly ConcurrentDictionary<BucketKey, IBucket> buckets;
        private readonly string metricsRoutingKey;
        private Borders borders;
        private readonly ILog log;
        private readonly AggregatorMetrics aggregatorMetrics;

        public MetricAggregator(
            IMetricScope metricScope,
            IBucketKeyProvider bucketKeyProvider,
            IAirlockClient airlockClient,
            TimeSpan cooldownPeriod,
            Borders borders,
            string eventsRoutingKey,
            ILog log)
        {
            aggregatorMetrics = new AggregatorMetrics(metricScope.WithTags(new Dictionary<string, string>
            {
                {MetricsTagNames.Type, "aggregation"}, {"routingKey", eventsRoutingKey}
            }));
            this.bucketKeyProvider = bucketKeyProvider;
            this.airlockClient = airlockClient;
            this.cooldownPeriod = cooldownPeriod;
            this.borders = borders;
            this.log = log;
            metricsRoutingKey = RoutingKey.ReplaceSuffix(eventsRoutingKey, RoutingKey.MetricsSuffix);
            buckets = new ConcurrentDictionary<BucketKey, IBucket>();
        }

        public void ProcessMetricEvent(MetricEvent metricEvent)
        {
            //log.Debug($"ProcessMetricEvent {metricEvent.Timestamp}, sum={metricEvent.Values.Values.Sum()}");
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
            log.Debug($"MetricAggregator.Flush {borders.Past} - {borders.Future}");
            foreach (var bucket in buckets)
            {
                var metrics = bucket.Value.Flush(nextBorders).ToArray();
                //log.Debug($"push bucket {bucket.Key}, metric sum = {metrics.SelectMany(m => m.Values.Values).Sum()}");
                PushToAirlock(metrics);
            }
        }

        private void PushToAirlock(IEnumerable<MetricEvent> metrics)
        {
            foreach (var metricEvent in metrics)
            {
                airlockClient.Push(metricsRoutingKey, metricEvent);
            }
        }

        public void Dispose()
        {
            aggregatorMetrics.Dispose();
        }
    }
}
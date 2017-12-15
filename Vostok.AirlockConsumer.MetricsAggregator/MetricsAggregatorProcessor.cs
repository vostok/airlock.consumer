using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Vostok.Airlock;
using Vostok.AirlockConsumer.MetricsAggregator.TracesToEvents;
using Vostok.Logging;
using Vostok.Metrics;
using Vostok.Metrics.Meters;

namespace Vostok.AirlockConsumer.MetricsAggregator
{
    public class MetricsAggregatorProcessor : IAirlockEventProcessor<MetricEvent>
    {
        private readonly MetricsAggregatorSettings settings;
        private readonly string eventsRoutingKey;
        private readonly IEventsTimestampProvider eventsTimestampProvider;
        private readonly IMetricAggregator aggregator;
        private readonly MetricResetDaemon resetDaemon;
        private readonly Task resetDaemonTask;
        private bool rebalanced;
        private readonly Borders initialBorders;

        public MetricsAggregatorProcessor(
            IAirlockClient airlockClient,
            IMetricScope rootMetricScope,
            MetricsAggregatorSettings settings,
            string eventsRoutingKey,
            ILog log)
        {
            this.settings = settings;
            this.eventsRoutingKey = eventsRoutingKey;
            eventsTimestampProvider = new EventsTimestampProvider(1000);
            initialBorders = CreateBorders(DateTimeOffset.UtcNow);
            aggregator = new MetricAggregator(
                rootMetricScope,
                new BucketKeyProvider(),
                airlockClient,
                settings.MetricAggregationPastGap,
                initialBorders,
                eventsRoutingKey, log);
            resetDaemon = new MetricResetDaemon(eventsTimestampProvider, settings, aggregator);
            resetDaemonTask = resetDaemon.StartAsync(initialBorders);
        }

        public DateTimeOffset? GetStartTimestampOnRebalance(string routingKey)
        {
            ValidateRoutingKey(routingKey);
            if (rebalanced)
                throw new InvalidOperationException($"Unexpected {nameof(GetStartTimestampOnRebalance)} call. Possible reason: many partitions for one topic are not supported for aggregation");
            rebalanced = true;
            return initialBorders.Past;
        }

        public void Process(List<AirlockEvent<MetricEvent>> events, ICounter messageProcessedCounter)
        {
            foreach (var consumerEvent in events)
            {
                ValidateRoutingKey(consumerEvent.RoutingKey);
                eventsTimestampProvider.AddTimestamp(consumerEvent.Payload.Timestamp);
                aggregator.ProcessMetricEvent(consumerEvent.Payload);
                messageProcessedCounter.Add();
            }
        }

        public void Release(string routingKey)
        {
            ValidateRoutingKey(routingKey);
            resetDaemon.Stop();
            resetDaemonTask?.GetAwaiter().GetResult();
        }

        private void ValidateRoutingKey(string routingKey)
        {
            if (routingKey != eventsRoutingKey)
                throw new InvalidOperationException($"routingKey({routingKey}) != eventsRoutingKey({eventsRoutingKey})");
        }

        private Borders CreateBorders(DateTimeOffset timestamp)
        {
            var future = timestamp + settings.MetricAggregationFutureGap;
            var past = timestamp - settings.MetricAggregationPastGap - settings.MetricAggregationStartGap;
            return new Borders(past, future);
        }
    }
}
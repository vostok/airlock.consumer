using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Vostok.Airlock;
using Vostok.Metrics;
using Vostok.Metrics.Meters;

namespace Vostok.AirlockConsumer.MetricsAggregator
{
    internal class MetricsAggregatorProcessor : IAirlockEventProcessor<MetricEvent>
    {
        private readonly IAirlockClient airlockClient;
        private readonly IMetricScope rootMetricScope;
        private readonly MetricsAggregatorSettings settings;
        private readonly string eventsRoutingKey;
        private IEventsTimestampProvider eventsTimestampProvider;
        private IMetricAggregator aggregator;
        private MetricResetDaemon resetDaemon;
        private Task resetDaemonTask;

        public MetricsAggregatorProcessor(
            IAirlockClient airlockClient,
            IMetricScope rootMetricScope,
            MetricsAggregatorSettings settings,
            string eventsRoutingKey)
        {
            this.airlockClient = airlockClient;
            this.rootMetricScope = rootMetricScope;
            this.settings = settings;
            this.eventsRoutingKey = eventsRoutingKey;
        }

        public DateTimeOffset? GetStartTimestampOnRebalance(string routingKey)
        {
            ValidateRoutingKey(routingKey);
            eventsTimestampProvider = new EventsTimestampProvider(1000);
            var initialBorders = CreateBorders(DateTimeOffset.UtcNow);
            aggregator = new MetricAggregator(
                rootMetricScope,
                new BucketKeyProvider(),
                airlockClient,
                settings.MetricAggregationPastGap,
                initialBorders,
                routingKey);
            resetDaemon = new MetricResetDaemon(eventsTimestampProvider, settings, aggregator);
            resetDaemonTask = resetDaemon.StartAsync(initialBorders);
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
            if (eventsRoutingKey != routingKey)
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
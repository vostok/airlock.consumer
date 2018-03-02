using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Vostok.Airlock.Metrics;
using Vostok.Airlock.Tracing;
using Vostok.Metrics;
using Vostok.Tracing;

namespace Vostok.Airlock.Consumer.MetricsAggregator
{
    public class MetricsAggregatorProcessor : IAirlockEventProcessor
    {
        private readonly MetricEventSerializer metricEventSerializer = new MetricEventSerializer();
        private readonly SpanAirlockSerializer spanAirlockSerializer = new SpanAirlockSerializer();
        private readonly IAirlockBatchClient airlockClient;
        private readonly IMetricScope rootMetricScope;
        private readonly MetricsAggregatorSettings settings;
        private readonly string eventsRoutingKey;
        private readonly ILog log;
        private IEventsTimestampProvider eventsTimestampProvider;
        private MetricAggregator aggregator;
        private MetricResetDaemon resetDaemon;
        private Task resetDaemonTask;
        private bool started;

        public MetricsAggregatorProcessor(
            IAirlockBatchClient airlockClient,
            IMetricScope rootMetricScope,
            MetricsAggregatorSettings settings,
            string eventsRoutingKey,
            ILog log)
        {
            this.airlockClient = airlockClient;
            this.rootMetricScope = rootMetricScope;
            this.settings = settings;
            this.eventsRoutingKey = eventsRoutingKey;
            this.log = log;
        }

        public string ProcessorId => nameof(MetricsAggregatorProcessor);

        public DateTimeOffset? GetStartTimestampOnRebalance(string routingKey)
        {
            ValidateRoutingKey(routingKey);
            if (started)
                throw new InvalidOperationException($"Unexpected {nameof(GetStartTimestampOnRebalance)} call. Possible reason: many partitions for one topic are not supported for aggregation");
            started = true;
            eventsTimestampProvider = new EventsTimestampProvider(1000);
            var initialBorders = CreateBorders(DateTimeOffset.UtcNow);
            aggregator = new MetricAggregator(
                rootMetricScope,
                new BucketKeyProvider(),
                airlockClient,
                settings.MetricAggregationPastGap,
                initialBorders,
                routingKey, log);
            resetDaemon = new MetricResetDaemon(eventsTimestampProvider, settings, aggregator);
            resetDaemonTask = resetDaemon.StartAsync(initialBorders);
            return initialBorders.Past;
        }

        public void Process(List<AirlockEvent<byte[]>> events, ProcessorMetrics processorMetrics)
        {
            foreach (var @event in events)
            {
                ValidateRoutingKey(@event.RoutingKey);
                var metricEvent = TryGetMetricEvent(@event);
                if (metricEvent != null)
                {
                    eventsTimestampProvider.AddTimestamp(metricEvent.Timestamp);
                    aggregator.ProcessMetricEvent(metricEvent);
                }
                processorMetrics.EventProcessedCounter.Add();
            }
        }

        public void Release(string routingKey)
        {
            ValidateRoutingKey(routingKey);
            resetDaemon.Stop();
            resetDaemonTask?.GetAwaiter().GetResult();
            aggregator.Dispose();
        }

        [CanBeNull]
        private MetricEvent TryGetMetricEvent(AirlockEvent<byte[]> airlockEvent)
        {
            if (RoutingKey.LastSuffixMatches(airlockEvent.RoutingKey, RoutingKey.AppEventsSuffix))
                return metricEventSerializer.Deserialize(new ByteBufferAirlockSource(airlockEvent.Payload));
            if (RoutingKey.LastSuffixMatches(airlockEvent.RoutingKey, RoutingKey.TracesSuffix))
            {
                var span = spanAirlockSerializer.Deserialize(new ByteBufferAirlockSource(airlockEvent.Payload));
                if (span.EndTimestamp.HasValue && span.Annotations.TryGetValue(TracingAnnotationNames.Kind, out var kind) && kind == "http-server")
                    return span.ToMetricEvent();
                return null;
            }
            throw new InvalidOperationException($"Payload type is not recognized for routingKey: {airlockEvent.RoutingKey}");
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
using System;
using System.Collections.Generic;
using System.Linq;
using Vostok.Airlock;
using Vostok.Logging;
using Vostok.Logging.Logs;
using Vostok.Metrics;
using Vostok.Metrics.Meters;
using Vostok.Tracing;

namespace Vostok.AirlockConsumer.MetricsAggregator.TracesToEvents
{
    public class HttpServerTracesProcessor : IAirlockEventProcessor<Span>
    {
        private readonly ILog log;
        private readonly MetricsAggregatorProcessor metricsAggregatorProcessor;
        private readonly string metricRoutingKey;

        public HttpServerTracesProcessor(IAirlockClient airlockClient, IMetricScope rootMetricScope, MetricsAggregatorSettings settings, string routingKey, ILog log)
        {
            this.log = log;
            metricRoutingKey = RoutingKey.ReplaceSuffix(routingKey, RoutingKey.TraceEventsSuffix);
            metricsAggregatorProcessor = new MetricsAggregatorProcessor(airlockClient, rootMetricScope, settings, metricRoutingKey, log);
        }

        public DateTimeOffset? GetStartTimestampOnRebalance(string routingKey)
        {
            return metricsAggregatorProcessor.GetStartTimestampOnRebalance(metricRoutingKey);
        }

        public void Process(List<AirlockEvent<Span>> events, ICounter messageProcessedCounter)
        {
            var httpServerSpanEvents = events
                .Where(x => x.Payload.Annotations.TryGetValue(TracingAnnotationNames.Kind, out var kind) && kind == "http-server")
                .Where(x => x.Payload.EndTimestamp.HasValue)
                .ToList();
            if (httpServerSpanEvents.Count == 0)
                return;
            var metricEvents = new List<AirlockEvent<MetricEvent>>();
            foreach (var @event in httpServerSpanEvents)
            {
                var metricEvent = MetricEventBuilder.Build(@event.Payload);
                metricEvents.Add(new AirlockEvent<MetricEvent> { Payload = metricEvent, RoutingKey = metricRoutingKey, Timestamp = metricEvent.Timestamp });
            }
            //log.Debug($"Send metrics traces processor to metric processor count={metricEvents.Count}, sum={metricEvents.SelectMany(x => x.Payload.Values.Values).Sum()}" );
            metricsAggregatorProcessor.Process(metricEvents, messageProcessedCounter);
        }

        public void Release(string routingKey)
        {
            metricsAggregatorProcessor.Release(metricRoutingKey);
        }
    }
}
using System.Collections.Generic;
using System.Linq;
using Vostok.Airlock;
using Vostok.Metrics.Meters;
using Vostok.Tracing;

namespace Vostok.AirlockConsumer.TracesToEvents
{
    // todo (spaceorc, 15.11.2017) удалить - научиться обрабатывать трассировки прямо в MetricsAggregator - чтобы сэкономить место в кафке https://github.com/vostok/airlock.consumer/issues/7
    public class TracesToEventsProcessor : SimpleAirlockEventProcessorBase<Span>
    {
        private readonly IAirlockClient airlockClient;

        public TracesToEventsProcessor(IAirlockClient airlockClient)
        {
            this.airlockClient = airlockClient;
        }

        public sealed override void Process(List<AirlockEvent<Span>> events, ICounter messageProcessedCounter)
        {
            var httpServerSpanEvents = events
                .Where(x => x.Payload.Annotations.TryGetValue("kind", out var kind) && kind == "http-server")
                .Where(x => x.Payload.EndTimestamp.HasValue)
                .ToList();
            foreach (var @event in httpServerSpanEvents)
            {
                var routingKey = RoutingKey.ReplaceSuffix(@event.RoutingKey, RoutingKey.TraceEventsSuffix);
                var metricEvent = MetricEventBuilder.Build(@event.Payload);
                airlockClient.Push(routingKey, metricEvent);
                messageProcessedCounter.Add();
            }
        }
    }
}
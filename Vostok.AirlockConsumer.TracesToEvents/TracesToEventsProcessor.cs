using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Vostok.Airlock;
using Vostok.Logging;
using Vostok.Tracing;

namespace Vostok.AirlockConsumer.TracesToEvents
{
    public class TracesToEventsProcessor : IAirlockEventProcessor<Span>
    {
        private readonly IAirlockClient airlockClient;
        private readonly ILog log;

        public TracesToEventsProcessor(IAirlockClient airlockClient, ILog log)
        {
            this.airlockClient = airlockClient;
            this.log = log;
        }

        public Task ProcessAsync(List<AirlockEvent<Span>> events)
        {
            var httpServerSpanEvents = events
                .Where(x => x.Payload.Annotations.TryGetValue("kind", out var kind) && kind == "http-server")
                .Where(x => x.Payload.EndTimestamp.HasValue)
                .ToList();

            log.Info($"Got {@events.Count} spans to aggregate, server spans: {httpServerSpanEvents.Count}");
            foreach (var @event in httpServerSpanEvents)
            {
                var routingKey = RoutingKey.ReplaceSuffix(@event.RoutingKey, RoutingKey.TraceEventsSuffix);
                var metricEvent = MetricEventBuilder.Build(@event.Payload);
                airlockClient.Push(routingKey, metricEvent);
            }

            return Task.CompletedTask;
        }
    }
}
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Vostok.Airlock;
using Vostok.Tracing;

namespace Vostok.AirlockConsumer.TracesToEvents
{
    public class TracesToEventsProcessor : IAirlockEventProcessor<Span>
    {
        private readonly IAirlockClient airlockClient;

        public TracesToEventsProcessor(IAirlockClient airlockClient)
        {
            this.airlockClient = airlockClient;
        }

        public Task ProcessAsync(List<AirlockEvent<Span>> events)
        {
            var httpServerSpanEvents = events
                .Where(x => x.Payload.Annotations.TryGetValue("kind", out var kind) && kind == "http-server")
                .Where(x => x.Payload.EndTimestamp.HasValue);
            foreach (var @event in httpServerSpanEvents)
            {
                var metricEvent = MetricEventBuilder.Build(@event.Payload);
                @event.Payload.Annotations.TryGetValue("serviceName", out var serviceName);

                var routingKey = TraceEventsRoutingKeyBuilder.Build(@event.RoutingKey, serviceName);
                airlockClient.Push(routingKey, metricEvent);
            }

            return Task.CompletedTask;
        }
    }
}
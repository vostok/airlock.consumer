using System.Collections.Generic;
using Vostok.Airlock;
using Vostok.Airlock.Metrics;
using Vostok.Airlock.Tracing;
using Vostok.Logging;
using Vostok.Tracing;

namespace Vostok.AirlockConsumer.TracesToEvents
{
    public class TracesToEventsEntryPoint : ConsumerApplication
    {
        public static void Main()
        {
            new ConsumerApplicationHost<TracesToEventsEntryPoint>().Run();
        }

        protected override string ServiceName => "traces-to-events";

        protected sealed override void DoInitialize(ILog log, IMetricScope rootMetricScope, Dictionary<string,string> environmentVariables, out IRoutingKeyFilter routingKeyFilter, out IAirlockEventProcessorProvider processorProvider)
        {
            routingKeyFilter = new DefaultRoutingKeyFilter(RoutingKey.TracesSuffix);
            AirlockSerializerRegistry.Register(new MetricEventSerializer());
            var airlockConfig = GetAirlockConfig(log, environmentVariables);
            var airlockClient = new AirlockClient(airlockConfig, log);
            processorProvider = new DefaultAirlockEventProcessorProvider<Span, SpanAirlockSerializer>(project => new TracesToEventsProcessor(airlockClient));
        }
    }
}
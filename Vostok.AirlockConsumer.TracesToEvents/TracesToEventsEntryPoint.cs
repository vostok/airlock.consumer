using System.Collections.Generic;
using Vostok.Airlock;
using Vostok.Airlock.Metrics;
using Vostok.Airlock.Tracing;
using Vostok.Logging;
using Vostok.Metrics;
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
        protected override ProcessorHostSettings ProcessorHostSettings => new ProcessorHostSettings
        {
            MaxBatchSize = 100000,
            MaxProcessorQueueSize = 1000000
        };

        protected sealed override void DoInitialize(ILog log, IMetricScope rootMetricScope, Dictionary<string, string> environmentVariables, out IRoutingKeyFilter routingKeyFilter, out IAirlockEventProcessorProvider processorProvider)
        {
            routingKeyFilter = new DefaultRoutingKeyFilter(RoutingKey.TracesSuffix);
            AirlockSerializerRegistry.Register(new MetricEventSerializer());
            processorProvider = new DefaultAirlockEventProcessorProvider<Span, SpanAirlockSerializer>(project => new TracesToEventsProcessor(AirlockClient));
        }
    }
}
using System.Collections.Generic;
using Vostok.Airlock;
using Vostok.Airlock.Logging;
using Vostok.Logging;
using Vostok.Metrics;

namespace Vostok.AirlockConsumer.Sentry
{
    public class SentryResenderEntryPoint : ConsumerApplication
    {
        private const string defaultSentryDsn = "http://88136054931b49089e319c8db2f8330c:30c7a8a830b142cc8270d544184ef0b8@vostok-sentry:9000/2";

        public static void Main()
        {
            new ConsumerApplicationHost<SentryResenderEntryPoint>().Run();
        }

        protected override string ServiceName => "consumer-sentry";
        protected override ProcessorHostSettings ProcessorHostSettings => new ProcessorHostSettings()
        {
            MaxBatchSize = 1000,
            MaxProcessorQueueSize = 100000
        };

        protected sealed override void DoInitialize(ILog log, IMetricScope rootMetricScope, Dictionary<string,string> environmentVariables, out IRoutingKeyFilter routingKeyFilter, out IAirlockEventProcessorProvider processorProvider)
        {
            routingKeyFilter = new DefaultRoutingKeyFilter(RoutingKey.LogsSuffix);
            var sentryDsn = GetSentryDsn(log, environmentVariables);
            processorProvider = new DefaultAirlockEventProcessorProvider<LogEventData, LogEventDataSerializer>(project => new SentryResenderProcessor(sentryDsn, log));
        }

        private static string GetSentryDsn(ILog log, Dictionary<string, string> environmentVariables)
        {
            if (!environmentVariables.TryGetValue("AIRLOCK_SENTRY_DSN", out var sentryDsn))
                sentryDsn = defaultSentryDsn;
            log.Info($"SentryDsn: {sentryDsn}");
            return sentryDsn;
        }
    }
}
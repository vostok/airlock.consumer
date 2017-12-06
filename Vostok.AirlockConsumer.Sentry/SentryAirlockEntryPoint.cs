using System.Collections.Generic;
using Vostok.Airlock;
using Vostok.Airlock.Logging;
using Vostok.Logging;
using Vostok.Metrics;

namespace Vostok.AirlockConsumer.Sentry
{
    public class SentryAirlockEntryPoint : ConsumerApplication
    {
        private const string defaultSentryUrl = "http://vostok-sentry:9000";
        private const string defaultSentryToken = "f61df24ac3864c55bbda24bfe68aea0c051ed4786d13475c93dd6d1534280a75";

        public static void Main()
        {
            new ConsumerApplicationHost<SentryAirlockEntryPoint>().Run();
        }

        protected override string ServiceName => "consumer-sentry";
        protected override ProcessorHostSettings ProcessorHostSettings => new ProcessorHostSettings
        {
            MaxBatchSize = SentryMaxTasks * 10,
            MaxProcessorQueueSize = SentryMaxTasks * 100
        };

        protected sealed override void DoInitialize(ILog log, IMetricScope rootMetricScope, Dictionary<string,string> environmentVariables, out IRoutingKeyFilter routingKeyFilter, out IAirlockEventProcessorProvider processorProvider)
        {
            routingKeyFilter = new DefaultRoutingKeyFilter(RoutingKey.LogsSuffix);
            var sentryApiClient = new SentryApiClient(GetSettingByName("SENTRY_URL", defaultSentryUrl), GetSettingByName("SENTRY_TOKEN", defaultSentryToken));
            var sentryClientProvider = new SentryClientProvider(sentryApiClient);
            var maxTasks = SentryMaxTasks;
            processorProvider = new SentryAirlockProcessorProvider<LogEventData, LogEventDataSerializer>((project,env) =>
            {
                var ravenClient = sentryClientProvider.GetOrCreateClient($"{project}_{env}");
                return new SentryAirlockProcessor(ravenClient, log, maxTasks);
            });
        }

        private int? sentryMaxTasks;
        private int SentryMaxTasks => sentryMaxTasks ?? (sentryMaxTasks = int.Parse(GetSettingByName("SENTRY_MAX_TASKS", "100"))).Value;
    }
}
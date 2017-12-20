using Vostok.Airlock;
using Vostok.Commons.Extensions.UnitConvertions;
using Vostok.Logging;
using Vostok.Metrics;

namespace Vostok.AirlockConsumer.Sentry
{
    public class SentryAirlockEntryPoint : ConsumerApplication
    {
        private const int sentryMaxTasks = 100;
        private const string defaultSentryUrl = "http://vostok-sentry:9000";
        private const string defaultSentryToken = "f61df24ac3864c55bbda24bfe68aea0c051ed4786d13475c93dd6d1534280a75";

        public static void Main()
        {
            new ConsumerApplicationHost<SentryAirlockEntryPoint>().Run();
        }

        protected override string ServiceName => "consumer-sentry";

        protected override ProcessorHostSettings ProcessorHostSettings => new ProcessorHostSettings
        {
            MaxBatchSize = sentryMaxTasks * 10,
            MaxProcessorQueueSize = sentryMaxTasks * 100
        };

        protected sealed override void DoInitialize(ILog log, IMetricScope rootMetricScope, AirlockEnvironmentVariables environmentVariables, out IRoutingKeyFilter routingKeyFilter, out IAirlockEventProcessorProvider processorProvider)
        {
            routingKeyFilter = new DefaultRoutingKeyFilter(RoutingKey.LogsSuffix);
            var sentryApiClientSettings = GetSentryApiClientSettings(log, environmentVariables);
            var sentryApiClient = new SentryApiClient(sentryApiClientSettings);
            processorProvider = new SentryAirlockProcessorProvider(sentryApiClient, log, sentryMaxTasks, 1.Minutes(), 100);
        }

        private static SentryApiClientSettings GetSentryApiClientSettings(ILog log, AirlockEnvironmentVariables environmentVariables)
        {
            var sentryUrl = environmentVariables.GetValue("SENTRY_URL", defaultSentryUrl);
            var sentryToken = environmentVariables.GetValue("SENTRY_TOKEN", defaultSentryToken);
            var sentryApiClientSettings = new SentryApiClientSettings
            {
                Url = sentryUrl,
                Token = sentryToken,
                Organization = "sentry",
            };
            log.Info($"SentryApiClientSettings: {sentryApiClientSettings.ToPrettyJson()}");
            return sentryApiClientSettings;
        }
    }
}
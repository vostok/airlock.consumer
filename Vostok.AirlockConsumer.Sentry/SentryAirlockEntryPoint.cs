using Vostok.Airlock;
using Vostok.Logging;
using Vostok.Metrics;

namespace Vostok.AirlockConsumer.Sentry
{
    public class SentryAirlockEntryPoint : ConsumerApplication
    {
        private SentryProcessorSettings sentryProcessorSettings;
        private const string defaultSentryUrl = "http://vostok-sentry:9000";
        private const string defaultSentryToken = "f61df24ac3864c55bbda24bfe68aea0c051ed4786d13475c93dd6d1534280a75";

        public static void Main()
        {
            new ConsumerApplicationHost<SentryAirlockEntryPoint>().Run();
        }

        protected override string ServiceName => "consumer-sentry";

        protected override ProcessorHostSettings ProcessorHostSettings => new ProcessorHostSettings
        {
            MaxBatchSize = 10_000,
            MaxProcessorQueueSize = 100_000
        };

        protected sealed override void DoInitialize(ILog log, IMetricScope rootMetricScope, AirlockEnvironmentVariables environmentVariables, out IRoutingKeyFilter routingKeyFilter, out IAirlockEventProcessorProvider processorProvider)
        {
            routingKeyFilter = new DefaultRoutingKeyFilter(RoutingKey.LogsSuffix);
            var sentryApiClientSettings = GetSentryApiClientSettings(log, environmentVariables);
            var sentryApiClient = new SentryApiClient(sentryApiClientSettings, log);
            sentryProcessorSettings = GetSentryProcessorSettings(log, environmentVariables);
            processorProvider = new SentryAirlockProcessorProvider(sentryApiClient, log, sentryProcessorSettings);
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

        private static SentryProcessorSettings GetSentryProcessorSettings(ILog log, AirlockEnvironmentVariables environmentVariables)
        {
            var sentryProcessorSettings = new SentryProcessorSettings();
            sentryProcessorSettings.MaxTasks = environmentVariables.GetIntValue("SENTRY_MAX_TASKS", sentryProcessorSettings.MaxTasks);
            sentryProcessorSettings.ThrottlingPeriod = environmentVariables.GetTimespan("SENTRY_THROTTLING_PERIOD", sentryProcessorSettings.ThrottlingPeriod);
            sentryProcessorSettings.ThrottlingThreshold = environmentVariables.GetIntValue("SENTRY_THROTTLING_THRESHOLD", sentryProcessorSettings.ThrottlingThreshold);
            log.Info($"SentryProcessorSettings: {sentryProcessorSettings.ToPrettyJson()}");
            return sentryProcessorSettings;
        }
    }
}
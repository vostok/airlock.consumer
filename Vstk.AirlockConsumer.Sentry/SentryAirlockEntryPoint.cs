using System;
using Vstk.Airlock;
using Vstk.Logging;
using Vstk.Metrics;

namespace Vstk.AirlockConsumer.Sentry
{
    public class SentryAirlockEntryPoint : ConsumerApplication
    {
        private const string defaultSentryUrl = "http://vstk-sentry:9000";
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
            var sentryProcessorSettings = GetSentryProcessorSettings(log, environmentVariables);
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
            var sentryProcessorSettings = new SentryProcessorSettings
            {
                MaxTasks = environmentVariables.GetIntValue("SENTRY_MAX_TASKS", 100),
                ThrottlingPeriod = environmentVariables.GetTimeSpanValue("SENTRY_THROTTLING_PERIOD", TimeSpan.FromMinutes(1)),
                ThrottlingThreshold = environmentVariables.GetIntValue("SENTRY_THROTTLING_THRESHOLD", 100)
            };
            log.Info($"SentryProcessorSettings: {sentryProcessorSettings.ToPrettyJson()}");
            return sentryProcessorSettings;
        }
    }
}
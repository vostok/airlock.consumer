using System;
using System.Linq;
using System.Net;
using Vostok.Airlock;
using Vostok.Airlock.Metrics;
using Vostok.Clusterclient.Topology;
using Vostok.Logging;
using Vostok.Metrics;

namespace Vostok.AirlockConsumer
{
    public abstract class ConsumerApplication : IDisposable
    {
        private const string defaultKafkaBootstrapEndpoints = "kafka:9092";
        private const string defaultAirlockGateEndpoints = "http://gate:6306";
        private const string defaultAirlockGateApiKey = "UniversalApiKey";
        protected IAirlockClient AirlockClient;
        private ConsumerMetrics consumerMetrics;

        protected abstract string ServiceName { get; }

        protected abstract ProcessorHostSettings ProcessorHostSettings { get; }

        public ConsumerGroupHost Initialize(ILog log, AirlockEnvironmentVariables environmentVariables)
        {
            AirlockClient = CreateAirlockClient(log, environmentVariables);

            var environment = environmentVariables.GetValue("VOSTOK_ENV", "dev");
            var metricRoutingKeyPrefix = RoutingKey.CreatePrefix("vostok", environment, ServiceName);
            IMetricScope rootMetricScope = new RootMetricScope(
                new MetricConfiguration
                {
                    Reporter = new AirlockMetricReporter(AirlockClient, metricRoutingKeyPrefix)
                });
            var consumerGroupHostSettings = GetConsumerGroupHostSettings(log, environmentVariables);
            consumerMetrics = new ConsumerMetrics(consumerGroupHostSettings.FlushMetricsInterval, rootMetricScope);

            DoInitialize(log, rootMetricScope, environmentVariables, out var routingKeyFilter, out var processorProvider);

            return new ConsumerGroupHost(consumerGroupHostSettings, log, consumerMetrics, routingKeyFilter, processorProvider);
        }

        public void Dispose()
        {
            consumerMetrics?.Dispose();
            (AirlockClient as IDisposable)?.Dispose();
        }

        protected abstract void DoInitialize(ILog log, IMetricScope rootMetricScope, AirlockEnvironmentVariables environmentVariables, out IRoutingKeyFilter routingKeyFilter, out IAirlockEventProcessorProvider processorProvider);

        private static IAirlockClient CreateAirlockClient(ILog log, AirlockEnvironmentVariables environmentVariables)
        {
            var airlockConfig = GetAirlockConfig(log, environmentVariables);
            var airlockClientLog = Logging.Configure("./log/airlock-{Date}.log", writeToConsole: false);
            return new AirlockClient(airlockConfig, airlockClientLog);
        }

        private static AirlockConfig GetAirlockConfig(ILog log, AirlockEnvironmentVariables environmentVariables)
        {
            var airlockGateApiKey = environmentVariables.GetValue("GATE_API_KEY", defaultAirlockGateApiKey);
            var airlockGateEndpoints = environmentVariables.GetValue("GATE_ENDPOINTS", defaultAirlockGateEndpoints);
            var airlockGateUris = airlockGateEndpoints.Split(new[] {';'}, StringSplitOptions.RemoveEmptyEntries).Select(x => new Uri(x)).ToArray();
            var airlockConfig = new AirlockConfig
            {
                ApiKey = airlockGateApiKey,
                ClusterProvider = new FixedClusterProvider(airlockGateUris),
            };
            log.Info($"AirlockConfig: {airlockConfig.ToPrettyJson()}");
            return airlockConfig;
        }

        private ConsumerGroupHostSettings GetConsumerGroupHostSettings(ILog log, AirlockEnvironmentVariables environmentVariables)
        {
            var consumerGroupId = environmentVariables.GetValue("CONSUMER_GROUP_ID", $"{GetType().Name}@{Dns.GetHostName()}");
            var kafkaBootstrapEndpoints = environmentVariables.GetValue("KAFKA_BOOTSTRAP_ENDPOINTS", defaultKafkaBootstrapEndpoints);
            var autoResetOffsetPolicy = environmentVariables.GetValue("KAFKA_AUTO_OFFSET_RESET", AutoResetOffsetPolicy.Latest);
            var consumerGroupHostSettings = new ConsumerGroupHostSettings(kafkaBootstrapEndpoints, consumerGroupId, ProcessorHostSettings, autoResetOffsetPolicy);
            log.Info($"ConsumerGroupHostSettings: {consumerGroupHostSettings.ToPrettyJson()}");
            return consumerGroupHostSettings;
        }
    }
}
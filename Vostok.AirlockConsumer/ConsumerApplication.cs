using System;
using System.Collections.Generic;
using System.Net;
using Vostok.Airlock;
using Vostok.Airlock.Metrics;
using Vostok.Logging;
using Vostok.Metrics;

namespace Vostok.AirlockConsumer
{
    public abstract class ConsumerApplication : IDisposable
    {
        private const string defaultKafkaBootstrapEndpoints = "kafka:9092";
        protected IAirlockClient AirlockClient;
        private ConsumerMetrics consumerMetrics;

        protected abstract string ServiceName { get; }
        protected abstract ProcessorHostSettings ProcessorHostSettings { get; }

        public ConsumerGroupHost Initialize(ILog log)
        {
            var environmentVariables = EnvironmentVariablesFactory.GetEnvironmentVariables(log);
            if (!environmentVariables.TryGetValue("VOSTOK_ENV", out var envName))
                envName = "dev";

            AirlockClient = AirlockClientFactory.CreateAirlockClient(environmentVariables, log);

            IMetricScope rootMetricScope = new RootMetricScope(
                new MetricConfiguration
                {
                    Reporter = new AirlockMetricReporter(AirlockClient, RoutingKey.CreatePrefix("vostok", envName, ServiceName))
                });
            var consumerGroupHostSettings = GetConsumerGroupHostSettings(log, environmentVariables, ProcessorHostSettings);
            consumerMetrics = new ConsumerMetrics(consumerGroupHostSettings.FlushMetricsInterval, rootMetricScope);

            DoInitialize(log, rootMetricScope, environmentVariables, out var routingKeyFilter, out var processorProvider);

            return new ConsumerGroupHost(consumerGroupHostSettings, log, consumerMetrics, routingKeyFilter, processorProvider);
        }

        public void Dispose()
        {
            consumerMetrics?.Dispose();
            (AirlockClient as IDisposable)?.Dispose();
        }

        protected abstract void DoInitialize(ILog log, IMetricScope rootMetricScope, Dictionary<string, string> environmentVariables, out IRoutingKeyFilter routingKeyFilter, out IAirlockEventProcessorProvider processorProvider);

        private ConsumerGroupHostSettings GetConsumerGroupHostSettings(ILog log, Dictionary<string, string> environmentVariables, ProcessorHostSettings processorHostSettings)
        {
            var consumerGroupId = GetConsumerGroupId(environmentVariables);
            var kafkaBootstrapEndpoints = GetKafkaBootstrapEndpoints(environmentVariables);
            var consumerGroupHostSettings = new ConsumerGroupHostSettings(kafkaBootstrapEndpoints, consumerGroupId, processorHostSettings);
            log.Info($"ConsumerGroupHostSettings: {consumerGroupHostSettings.ToPrettyJson()}");
            return consumerGroupHostSettings;
        }

        private string GetConsumerGroupId(Dictionary<string, string> environmentVariables)
        {
            if (!environmentVariables.TryGetValue("AIRLOCK_CONSUMER_GROUP_ID", out var consumerGroupId))
                consumerGroupId = $"{GetType().Name}@{Dns.GetHostName()}";
            return consumerGroupId;
        }

        private static string GetKafkaBootstrapEndpoints(Dictionary<string, string> environmentVariables)
        {
            if (!environmentVariables.TryGetValue("AIRLOCK_KAFKA_BOOTSTRAP_ENDPOINTS", out var kafkaBootstrapEndpoints))
                kafkaBootstrapEndpoints = defaultKafkaBootstrapEndpoints;
            return kafkaBootstrapEndpoints;
        }
    }
}
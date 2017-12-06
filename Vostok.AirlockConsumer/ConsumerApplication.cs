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
        private Dictionary<string, string> environmentVariables;
        private ILog log;

        protected abstract string ServiceName { get; }
        protected abstract ProcessorHostSettings ProcessorHostSettings { get; }

        public ConsumerGroupHost Initialize(ILog logValue)
        {
            log = logValue;
            environmentVariables = EnvironmentVariablesFactory.GetEnvironmentVariables(logValue);
            if (!environmentVariables.TryGetValue("VOSTOK_ENV", out var envName))
                envName = "dev";

            AirlockClient = AirlockClientFactory.CreateAirlockClient(environmentVariables, logValue);

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

        private ConsumerGroupHostSettings GetConsumerGroupHostSettings(ProcessorHostSettings processorHostSettings)
        {
            var consumerGroupId = GetSettingByName("CONSUMER_GROUP_ID", $"{GetType().Name}@{Dns.GetHostName()}");
            var kafkaBootstrapEndpoints = GetSettingByName("KAFKA_BOOTSTRAP_ENDPOINTS", defaultKafkaBootstrapEndpoints);
            var consumerGroupHostSettings = new ConsumerGroupHostSettings(kafkaBootstrapEndpoints, consumerGroupId, processorHostSettings);
            log.Info($"ConsumerGroupHostSettings: {consumerGroupHostSettings.ToPrettyJson()}");
            return consumerGroupHostSettings;
        }

        protected string GetSettingByName(string name, string defaultValue = null)
        {
            if (!environmentVariables.TryGetValue("AIRLOCK_" + name, out var value))
                value = defaultValue;
            log.Info($"{name}: {value}");
            return value;
        }
    }
}
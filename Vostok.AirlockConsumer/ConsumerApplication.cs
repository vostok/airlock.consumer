using System;
using System.Net;
using Vostok.Airlock;
using Vostok.Graphite.Client;
using Vostok.Graphite.Reporter;
using Vostok.Logging;
using Vostok.Metrics;

namespace Vostok.AirlockConsumer
{
    public abstract class ConsumerApplication : IDisposable
    {
        private const string defaultKafkaBootstrapEndpoints = "kafka:9092";
        private const string defaultGraphiteEndpoint = "graphite:2003";
        private ConsumerMetrics consumerMetrics;
        private GraphiteSink graphiteSink;

        protected abstract string ServiceName { get; }

        protected abstract ProcessorHostSettings ProcessorHostSettings { get; }

        public ConsumerGroupHost Initialize(ILog log, AirlockEnvironmentVariables environmentVariables)
        {
            var environment = environmentVariables.GetValue("VOSTOK_ENV", "dev");
            var metricRoutingKeyPrefix = RoutingKey.CreatePrefix("vostok", environment, ServiceName);
            var graphiteUri = GetGraphiteUri(log, environmentVariables);
            var graphiteSinkConfig = new GraphiteSinkConfig
            {
                GraphiteHost = graphiteUri.Host,
                GraphitePort = graphiteUri.Port
            };
            log.Info($"GraphiteSinkConfig: {graphiteSinkConfig.ToPrettyJson()}");
            graphiteSink = new GraphiteSink(graphiteSinkConfig, log);
            var rootMetricScope = new RootMetricScope(new MetricConfiguration
            {
                Reporter = new GraphiteMetricReporter(graphiteSink, metricRoutingKeyPrefix, log)
            });
            var consumerGroupHostSettings = GetConsumerGroupHostSettings(log, environmentVariables);
            consumerMetrics = new ConsumerMetrics(consumerGroupHostSettings.FlushMetricsInterval, rootMetricScope);

            DoInitialize(log, rootMetricScope, environmentVariables, out var routingKeyFilter, out var processorProvider);

            return new ConsumerGroupHost(consumerGroupHostSettings, log, consumerMetrics, routingKeyFilter, processorProvider);
        }

        public virtual void Dispose()
        {
            consumerMetrics?.Dispose();
            graphiteSink?.Dispose();
        }

        protected abstract void DoInitialize(ILog log, IMetricScope rootMetricScope, AirlockEnvironmentVariables environmentVariables, out IRoutingKeyFilter routingKeyFilter, out IAirlockEventProcessorProvider processorProvider);

        protected static Uri GetGraphiteUri(ILog log, AirlockEnvironmentVariables environmentVariables)
        {
            var graphiteEndpoint = environmentVariables.GetValue("GRAPHITE_ENDPOINT", defaultGraphiteEndpoint);
            var graphiteUri = new Uri($"tcp://{graphiteEndpoint}");
            log.Info($"GraphiteUri: {graphiteUri}");
            return graphiteUri;
        }

        private ConsumerGroupHostSettings GetConsumerGroupHostSettings(ILog log, AirlockEnvironmentVariables environmentVariables)
        {
            var consumerGroupId = environmentVariables.GetValue("CONSUMER_GROUP_ID", $"{GetType().Name}@{Dns.GetHostName()}");
            var kafkaBootstrapEndpoints = environmentVariables.GetValue("KAFKA_BOOTSTRAP_ENDPOINTS", defaultKafkaBootstrapEndpoints);
            var autoResetOffsetPolicy = environmentVariables.GetEnumValue("KAFKA_AUTO_OFFSET_RESET", AutoResetOffsetPolicy.Latest);
            var consumerGroupHostSettings = new ConsumerGroupHostSettings(kafkaBootstrapEndpoints, consumerGroupId, ProcessorHostSettings, autoResetOffsetPolicy);
            log.Info($"ConsumerGroupHostSettings: {consumerGroupHostSettings.ToPrettyJson()}");
            return consumerGroupHostSettings;
        }
    }
}
using System.Collections.Generic;
using System.Threading.Tasks;
using Vostok.Metrics;

namespace Vostok.AirlockConsumer.MetricsAggregator
{
    internal class MetricAirlockEventProcessor : IAirlockEventProcessor<MetricEvent>
    {
        private readonly IMetricAggregator metricAggregator;

        public MetricAirlockEventProcessor(IMetricAggregator metricAggregator)
        {
            this.metricAggregator = metricAggregator;
        }

        public Task ProcessAsync(List<AirlockEvent<MetricEvent>> events)
        {
            foreach (var consumerEvent in events)
                metricAggregator.ProcessMetricEvent(consumerEvent.RoutingKey, consumerEvent.Payload);
            return Task.CompletedTask;
        }
    }
}
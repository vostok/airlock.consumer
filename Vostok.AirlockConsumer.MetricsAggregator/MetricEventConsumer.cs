using Vostok.Logging;
using Vostok.Metrics;

namespace Vostok.AirlockConsumer.MetricsAggregator
{
    public class MetricEventConsumer : AirlockConsumer<MetricEvent>
    {
        public MetricEventConsumer(MetricsAggregatorSettings settings, IMessageProcessor<MetricEvent> messageProcessor)
            : base(4, settings.BatchSize, new MetricEventSerializer(), messageProcessor, Program.Log.ForContext<MetricEventConsumer>())
        {
        }
    }
}
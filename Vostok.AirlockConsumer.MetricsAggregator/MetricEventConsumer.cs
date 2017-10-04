using System.Collections.Generic;
using Vostok.Logging;
using Vostok.Metrics;

namespace Vostok.AirlockConsumer.MetricsAggregator
{
    public class MetricEventConsumer : AirlockConsumer<MetricEvent>
    {
        public MetricEventConsumer(Dictionary<string, object> settings, IMessageProcessor<MetricEvent> messageProcessor, ILog log)
            : base(
                settings,
                new[] {"vostok:staging:bluewater:events"},
                new MetricEventSerializer(),
                messageProcessor,
                log.ForContext<MetricEventConsumer>())
        {
        }
    }
}
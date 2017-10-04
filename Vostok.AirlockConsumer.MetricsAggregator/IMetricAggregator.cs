using Vostok.Metrics;

namespace Vostok.AirlockConsumer.MetricsAggregator
{
    internal interface IMetricAggregator
    {
        void ProcessMetricEvent(string routingKey, MetricEvent metricEvent);
        void Reset(Borders nextBorders);
    }
}
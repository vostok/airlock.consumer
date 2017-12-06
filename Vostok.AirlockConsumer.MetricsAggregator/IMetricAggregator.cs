using Vostok.Metrics;

namespace Vostok.AirlockConsumer.MetricsAggregator
{
    public interface IMetricAggregator
    {
        void ProcessMetricEvent(MetricEvent metricEvent);
        void Flush(Borders nextBorders);
    }
}
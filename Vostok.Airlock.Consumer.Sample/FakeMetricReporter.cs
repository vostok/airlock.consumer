using Vostok.Metrics;

namespace Vostok.Airlock.Consumer.Sample
{
    public class FakeMetricReporter : IMetricEventReporter
    {
        public void SendEvent(MetricEvent metricEvent)
        {
        }

        public void SendMetric(MetricEvent metricEvent)
        {
        }
    }
}
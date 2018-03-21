using Vstk.Metrics;

namespace Vstk.AirlockConsumer.Sample
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
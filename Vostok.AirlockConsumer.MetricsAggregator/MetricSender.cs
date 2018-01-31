using Vostok.Graphite.Client;
using Vostok.Graphite.Reporter;
using Vostok.Logging;
using Vostok.Metrics;

namespace Vostok.AirlockConsumer.MetricsAggregator
{
    public interface IMetricSender
    {
        void SendMetric(string routingKey, MetricEvent metricEvent);
    }

    public class MetricSender : IMetricSender
    {
        private readonly IGraphiteSink sink;
        private readonly MetricConverter metricConverter;

        public MetricSender(IGraphiteSink sink, ILog log)
        {
            this.sink = sink;
            metricConverter = new MetricConverter(new GraphiteNameBuilder(), log);
        }

        public void SendMetric(string routingKey, MetricEvent metricEvent)
        {
            var metrics = metricConverter.Convert(routingKey, metricEvent);
            sink.Push(metrics);
        }
    }
}
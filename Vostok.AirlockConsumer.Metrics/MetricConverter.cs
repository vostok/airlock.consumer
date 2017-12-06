using System.Collections.Generic;
using Vostok.Graphite.Client;
using Vostok.Metrics;

namespace Vostok.AirlockConsumer.Metrics
{
    public class MetricConverter
    {
        private readonly IGraphiteNameBuilder graphiteNameBuilder;

        public MetricConverter(IGraphiteNameBuilder graphiteNameBuilder)
        {
            this.graphiteNameBuilder = graphiteNameBuilder;
        }

        public IEnumerable<Metric> Convert(string routingKey, MetricEvent metricEvent)
        {
            var prefix = graphiteNameBuilder.BuildPrefix(routingKey, metricEvent.Tags);
            foreach (var pair in metricEvent.Values)
            {
                var name = graphiteNameBuilder.BuildName(prefix, pair.Key);
                var timestamp = metricEvent.Timestamp.ToUnixTimeSeconds();
                yield return new Metric(name, pair.Value, timestamp);
            }
        }
    }
}
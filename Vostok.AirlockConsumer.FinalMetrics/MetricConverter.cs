using System;
using System.Collections.Generic;
using Vostok.GraphiteClient;
using Vostok.Metrics;

namespace Vostok.AirlockConsumer.FinalMetrics
{
    internal class MetricConverter
    {
        private readonly IGraphiteNameBuilder graphiteNameBuilder;
        private readonly DateTime unixStartPeriod = new DateTime(1970, 01, 01);

        public MetricConverter(IGraphiteNameBuilder graphiteNameBuilder)
        {
            this.graphiteNameBuilder = graphiteNameBuilder;
        }

        public IEnumerable<Metric> Convert(string routingKey, MetricEvent metricEvent)
        {
            var prefixName = graphiteNameBuilder.Build(routingKey, metricEvent.Tags);
            foreach (var pair in metricEvent.Values)
            {
                var name = graphiteNameBuilder.Build(prefixName, pair.Key);
                var timestamp = ToUnixTimestamp(metricEvent.Timestamp);
                yield return new Metric(name, pair.Value, timestamp);
            }
        }

        private long ToUnixTimestamp(DateTimeOffset dateTimeOffset)
        {
            return (long)(dateTimeOffset.UtcDateTime - unixStartPeriod).TotalSeconds;
        }
    }
}
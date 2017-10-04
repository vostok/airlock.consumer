using System;
using Vostok.Commons.Extensions.UnitConvertions;

namespace Vostok.AirlockConsumer.MetricsAggregator
{
    public class MetricsAggregatorSettings
    {
        public int BatchSize { get; set; } = 1000;
        public TimeSpan MetricAggregationPastGap { get; set; } = 20.Seconds();
        public TimeSpan MetricAggregationFutureGap { get; set; } = 1.Hours();
    }
}
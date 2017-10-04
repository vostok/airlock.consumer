using System;
using Vostok.Commons.Extensions.UnitConvertions;

namespace Vostok.AirlockConsumer.MetricsAggregator
{
    public class MetricsAggregatorSettings
    {
        public int BatchSize { get; set; } = 1000;
        public TimeSpan MetricAggregationGap { get; set; } = 20.Seconds();
    }
}
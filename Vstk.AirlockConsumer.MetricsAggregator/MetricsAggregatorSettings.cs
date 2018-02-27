using System;
using Vstk.Commons.Extensions.UnitConvertions;

namespace Vstk.AirlockConsumer.MetricsAggregator
{
    public class MetricsAggregatorSettings
    {
        public TimeSpan MetricAggregationPastGap { get; set; } = 20.Seconds();
        public TimeSpan MetricAggregationFutureGap { get; set; } = 1.Hours();
        public TimeSpan MetricAggregationStartGap { get; set; } = 10.Minutes();
        public TimeSpan MetricResetDaemonIterationPeriod { get; set; } = 5.Seconds();
    }
}
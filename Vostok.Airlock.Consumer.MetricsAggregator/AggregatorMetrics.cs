using System;
using Vostok.Commons.Extensions.UnitConvertions;
using Vostok.Metrics;
using Vostok.Metrics.Meters;

namespace Vostok.Airlock.Consumer.MetricsAggregator
{
    public class AggregatorMetrics : IDisposable
    {
        public AggregatorMetrics(IMetricScope metricScope)
        {
            MissedPastEvents = metricScope.Counter(1.Minutes(), "missed_past_events");
            MissedFutureEvents = metricScope.Counter(1.Minutes(), "missed_future_events");
        }

        public ICounter MissedPastEvents { get; }
        public ICounter MissedFutureEvents { get; }

        public void Dispose()
        {
            (MissedPastEvents as IDisposable)?.Dispose();
            (MissedFutureEvents as IDisposable)?.Dispose();
        }
    }
}
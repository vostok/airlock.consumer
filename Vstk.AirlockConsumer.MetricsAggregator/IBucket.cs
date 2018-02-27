using System;
using System.Collections.Generic;
using Vstk.Metrics;

namespace Vstk.AirlockConsumer.MetricsAggregator
{
    public interface IBucket
    {
        void Consume(IReadOnlyDictionary<string, double> values, DateTimeOffset timestamp);
        IEnumerable<MetricEvent> Flush(Borders nextBorders);
    }
}
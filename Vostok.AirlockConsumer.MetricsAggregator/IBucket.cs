using System;
using System.Collections.Generic;
using Vostok.Metrics;

namespace Vostok.AirlockConsumer.MetricsAggregator
{
    internal interface IBucket
    {
        void Consume(IReadOnlyDictionary<string, double> values, DateTimeOffset timestamp);
        IEnumerable<MetricEvent> Reset(Borders nextBorders);
    }
}
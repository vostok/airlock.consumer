using System;
using System.Collections.Generic;
using Vostok.Metrics;

namespace Vostok.AirlockConsumer.MetricsAggregator
{
    internal interface IBucket
    {
        string RoutingKey { get; }
        TimeSpan Period { get; }
        IReadOnlyDictionary<string, string> Tags { get; }
        void Consume(IReadOnlyDictionary<string, double> values, DateTimeOffset timestamp);
        IEnumerable<MetricEvent> Reset(DateTimeOffset border);
    }
}
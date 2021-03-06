﻿using System;
using System.Collections.Generic;
using Vostok.Metrics;

namespace Vostok.Airlock.Consumer.MetricsAggregator
{
    public interface IBucket
    {
        void Consume(IReadOnlyDictionary<string, double> values, DateTimeOffset timestamp);
        IEnumerable<MetricEvent> Flush(Borders nextBorders);
    }
}
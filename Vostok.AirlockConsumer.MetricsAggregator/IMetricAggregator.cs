﻿using Vostok.Metrics;

namespace Vostok.AirlockConsumer.MetricsAggregator
{
    internal interface IMetricAggregator
    {
        void ProcessMetricEvent(MetricEvent metricEvent);
        void Flush(Borders nextBorders);
    }
}
﻿using System.Collections.Generic;
using Vostok.Metrics;

namespace Vostok.AirlockConsumer.MetricsAggregator
{
    internal class MetricEventProcessor : IMessageProcessor<MetricEvent>
    {
        private readonly IMetricAggregator metricAggregator;

        public MetricEventProcessor(IMetricAggregator metricAggregator)
        {
            this.metricAggregator = metricAggregator;
        }

        public void Process(IEnumerable<ConsumerEvent<MetricEvent>> events)
        {
            foreach (var consumerEvent in events)
            {
                metricAggregator.ProcessMetricEvent(consumerEvent.Project, consumerEvent.Event);
            }
        }
    }
}
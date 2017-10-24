using System;
using System.Collections.Concurrent;
using Vostok.Metrics;
using Vostok.Metrics.Meters;

namespace Vostok.AirlockConsumer
{
    internal class ConsumerMetrics
    {
        public int QueueSize { get; set; }
        public int RebalanceAge { get; set; }
        public int RebalanceCnt { get; set; }
        public int AssignmentSize { get; set; }
        public int PausedProcessors { private get; set; }
        public int ProcessorCount { private get; set; }

        public readonly TimeSpan MetricsFlushPeriod = TimeSpan.FromSeconds(5);
        public readonly ICounter MessagesCounter;
        public readonly ICounter CriticalErrorCounter;
        public readonly ICounter ConsumeErrorCounter;
        private readonly IMetricScope processorsScope;
        private readonly ConcurrentDictionary<string, int> processorQueueSize = new ConcurrentDictionary<string, int>();

        public ConsumerMetrics(IMetricScope rootMetricScope)
        {
            var gaugesScope = rootMetricScope.WithTag(MetricsTagNames.Type, "gauges");
            gaugesScope.Gauge(MetricsFlushPeriod, "queue-size", () => QueueSize);
            gaugesScope.Gauge(MetricsFlushPeriod, "rebalance-age", () => RebalanceAge);
            gaugesScope.Gauge(MetricsFlushPeriod, "rebalance-cnt", () => RebalanceCnt);
            gaugesScope.Gauge(MetricsFlushPeriod, "assignment-size", () => AssignmentSize);
            gaugesScope.Gauge(MetricsFlushPeriod, "paused-processors", () => PausedProcessors);
            gaugesScope.Gauge(MetricsFlushPeriod, "processor-count", () => ProcessorCount);
            processorsScope = rootMetricScope.WithTag(MetricsTagNames.Type, "processors");
            MessagesCounter = rootMetricScope.Counter(MetricsFlushPeriod, "messages");
            var errorsScope = rootMetricScope.WithTag(MetricsTagNames.Type, "error");
            CriticalErrorCounter = errorsScope.Counter(MetricsFlushPeriod, "critical");
            ConsumeErrorCounter = errorsScope.Counter(MetricsFlushPeriod, "consume");
        }

        public void SetProcessorQueueSize(string routingKey, int size)
        {
            processorQueueSize.AddOrUpdate(routingKey, k =>
            {
                processorsScope.Gauge(MetricsFlushPeriod, routingKey,
                    () =>
                    {
                        if (!processorQueueSize.TryGetValue(routingKey, out var curSize))
                            curSize = 0;
                        return curSize;
                    });
                return size;
            }, (k, v) => size);
        }
    }
}
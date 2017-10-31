using System;
using System.Threading;
using Newtonsoft.Json.Linq;
using Vostok.Metrics;
using Vostok.Metrics.Meters;

namespace Vostok.AirlockConsumer
{
    internal class ConsumerMetrics
    {
        public readonly ICounter CriticalErrorCounter;
        public readonly ICounter ConsumeErrorCounter;

        private readonly IMetricScope rootMetricScope;
        private readonly IMetricScope processorsScope;
        private readonly IMetricScope statScope;
        private readonly Counter messagesCounter = new Counter();

        private struct KafkaStat
        {
            public int QueueSize;
            public int AssignmentSize;
            public int RebalanceAge;
            public int RebalanceCnt;
        }

        private KafkaStat stat;
        public int ProcessorCount;

        public ConsumerMetrics(TimeSpan flushMetricsInterval, IMetricScope rootMetricScope)
        {
            this.rootMetricScope = rootMetricScope;
            statScope = rootMetricScope.WithTag(MetricsTagNames.Type, "stat");
            processorsScope = rootMetricScope.WithTag(MetricsTagNames.Type, "processors");
            var errorsScope = rootMetricScope.WithTag(MetricsTagNames.Type, "error");
            CriticalErrorCounter = errorsScope.Counter(flushMetricsInterval, "critical");
            ConsumeErrorCounter = errorsScope.Counter(flushMetricsInterval, "consume");

            MetricClocks.Get(flushMetricsInterval).Register(WriteMetrics);
        }

        public IMetricScope GetProcessorScope(string routingKey)
        {
            return processorsScope.WithTag(MetricsTagNames.Operation, routingKey);
        }

        private void WriteMetrics(DateTimeOffset timeStamp)
        {
            Thread.MemoryBarrier();
            statScope
                .WriteMetric()
                .SetTimestamp(timeStamp)
                .SetValue("queue_size", stat.QueueSize)
                .SetValue("assignment_size", stat.AssignmentSize)
                .SetValue("rebalance_age", stat.RebalanceAge)
                .SetValue("rebalance_cnt", stat.RebalanceCnt)
                .Commit();
            rootMetricScope
                .WriteMetric()
                .SetTimestamp(timeStamp)
                .SetValue("processor_count", ProcessorCount)
                .SetValue("message_count", messagesCounter.Reset())
                .Commit();
        }

        public void UpdateKafkaStat(string statJson)
        {
            dynamic jStat = JObject.Parse(statJson);
            stat.QueueSize = jStat.replyq;
            stat.AssignmentSize = jStat.cgrp.assignment_size;
            stat.RebalanceAge = jStat.cgrp.rebalance_age;
            stat.RebalanceCnt = jStat.cgrp.rebalance_cnt;
        }

        public void IncrementMessage()
        {
            messagesCounter.Add();
        }
    }
}
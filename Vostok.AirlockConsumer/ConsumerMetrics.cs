using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json.Linq;
using Vostok.Metrics;
using Vostok.Metrics.Meters;

namespace Vostok.AirlockConsumer
{
    internal class ConsumerMetrics
    {
        public readonly ICounter CriticalErrorCounter;
        public readonly ICounter ConsumeErrorCounter;

        private readonly TimeSpan flushMetricsInterval;
        private readonly IMetricScope rootMetricScope;
        private readonly IMetricScope processorsScope;
        private readonly IMetricScope statScope;
        private int messagesCounter;
        private readonly Dictionary<string,int> countersByRoutingKey = new Dictionary<string, int>();

        public ConsumerMetrics(TimeSpan flushMetricsInterval, IMetricScope rootMetricScope)
        {
            this.flushMetricsInterval = flushMetricsInterval;
            this.rootMetricScope = rootMetricScope;
            statScope = rootMetricScope.WithTag(MetricsTagNames.Type, "stat");
            processorsScope = rootMetricScope.WithTag(MetricsTagNames.Type, "processors");
            var errorsScope = rootMetricScope.WithTag(MetricsTagNames.Type, "error");
            CriticalErrorCounter = errorsScope.Counter(this.flushMetricsInterval, "critical");
            ConsumeErrorCounter = errorsScope.Counter(this.flushMetricsInterval, "consume");
        }

        public void WriteKafkaStat(string statJson)
        {
            dynamic jStat = JObject.Parse(statJson);
            var now = DateTimeOffset.UtcNow;
            statScope
                .WriteMetric()
                .SetTimestamp(now)
                .SetValue("queue_size", jStat.replyq)
                .SetValue("assignment_size", jStat.cgrp.assignment_size)
                .SetValue("rebalance_age", jStat.cgrp.rebalance_age)
                .SetValue("rebalance_cnt", jStat.cgrp.rebalance_cnt)
                .Commit();
        }

        public void IncrementMessage(string routingKey)
        {
            messagesCounter++;
            if (!countersByRoutingKey.TryGetValue(routingKey, out var counter))
                counter = 0;
            countersByRoutingKey[routingKey] = counter + 1;
        }

        public void WriteProcessorMetric(Dictionary<string, (IAirlockEventProcessor Processor, ProcessorHost ProcessorHost)> processorInfos)
        {
            var now = GetNormalizedNow();
            processorsScope.WriteMetric().SetTimestamp(now)
                .SetValue("count", processorInfos.Count)
                .SetValue("paused", processorInfos.Values.Count(x => x.ProcessorHost.Paused))
                .Commit();
            rootMetricScope.WriteMetric().SetTimestamp(now).SetValue("message-count", messagesCounter).Commit();
            foreach (var processorInfoKv in processorInfos)
            {
                var processorInfo = processorInfoKv.Value;
                processorsScope.WriteMetric().SetTimestamp(now).SetTag("routingKey", processorInfoKv.Key).SetValue("queue-size", processorInfo.ProcessorHost.QueueSize).Commit();
            }
            foreach (var counterKv in countersByRoutingKey.ToArray())
            {
                if (counterKv.Value <= 0)
                    countersByRoutingKey.Remove(counterKv.Key);
                processorsScope.WriteMetric().SetTimestamp(now).SetTag("routingKey", counterKv.Key).SetValue("messages", counterKv.Value).Commit();
                if (counterKv.Value > 0)
                    countersByRoutingKey[counterKv.Key] = 0;
            }
            messagesCounter = 0;
        }

        private DateTimeOffset GetNormalizedNow()
        {
            var now = DateTimeOffset.UtcNow;
            return new DateTimeOffset(now.Ticks - now.Ticks % flushMetricsInterval.Ticks, TimeSpan.Zero);
        }
    }
}
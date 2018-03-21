using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using Vostok.Metrics;
using Vostok.Metrics.Meters;

namespace Vostok.AirlockConsumer.MetricsAggregator
{
    public class TimeBin
    {
        private readonly DateTimeOffset timestamp;
        private readonly ConcurrentDictionary<string, Meter> meters;
        private readonly Counter counter;
        private long lastConsumeTimeUtcTicks;
        private long flushedEvents;

        public TimeBin(DateTimeOffset timestamp)
        {
            this.timestamp = timestamp;
            meters = new ConcurrentDictionary<string, Meter>();
            counter = new Counter();
        }

        public void Consume(IReadOnlyDictionary<string, double> values)
        {
            Interlocked.Exchange(ref lastConsumeTimeUtcTicks, DateTimeOffset.UtcNow.UtcTicks);
            counter.Add();
            foreach (var kvp in values)
            {
                var meter = meters.GetOrAdd(kvp.Key, _ => new Meter());
                meter.Add(kvp.Value);
            }
        }

        public MetricEvent TryFlush(IReadOnlyDictionary<string, string> tags, TimeSpan? cooldownPeriod = null)
        {
            if (cooldownPeriod.HasValue && GetLastConsumeTime() > DateTimeOffset.UtcNow - cooldownPeriod.Value)
                return null;
            var eventsCount = counter.GetValue();
            if (flushedEvents == eventsCount)
                return null;
            flushedEvents = eventsCount;
            return AggregateMetric(tags);
        }

        private MetricEvent AggregateMetric(IReadOnlyDictionary<string, string> tags)
        {
            var values = new Dictionary<string, double>
            {
                {"count", counter.GetValue()}
            };
            foreach (var m in meters)
            {
                var meterValues = m.Value.GetValues();
                foreach (var mv in meterValues)
                    values[m.Key + "_" + mv.Key] = mv.Value;
            }
            var metric = new MetricEvent
            {
                Timestamp = timestamp,
                Tags = tags,
                Values = values
            };
            return metric;
        }

        private DateTimeOffset GetLastConsumeTime() => new DateTimeOffset(Interlocked.Read(ref lastConsumeTimeUtcTicks), TimeSpan.Zero);
    }
}
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using Vostok.Commons.Extensions.UnitConvertions;
using Vostok.Metrics;
using Vostok.Metrics.Meters;

namespace Vostok.AirlockConsumer.MetricsAggregator
{
    //TODO @ezsilmar Unit test this
    internal class Bucket : IBucket
    {
        private readonly IReadOnlyDictionary<string, string> tags;
        private readonly TimeSpan period;
        private readonly TimeSpan cooldownPeriod;
        private readonly ConcurrentDictionary<DateTimeOffset, TimeBin> timeBins;
        private readonly ICounter missedPastEvents;
        private readonly ICounter missedFutureEvents;
        private Borders borders;

        public Bucket(
            IMetricScope metricScope,
            IReadOnlyDictionary<string, string> tags,
            TimeSpan period,
            TimeSpan cooldownPeriod,
            Borders borders)
        {
            this.tags = tags;
            this.period = period;
            this.cooldownPeriod = cooldownPeriod;
            timeBins = new ConcurrentDictionary<DateTimeOffset, TimeBin>();
            missedPastEvents = metricScope.Counter(1.Minutes(), "missed_past_events");
            missedFutureEvents = metricScope.Counter(1.Minutes(), "missed_future_events");
            this.borders = NormalizeBorders(borders);
        }

        public void Consume(IReadOnlyDictionary<string, double> values, DateTimeOffset timestamp)
        {
            var normalizedTimestamp = NormalizeTimestamp(timestamp);
            var currentBorders = Interlocked.CompareExchange(ref borders, null, null);

            if (normalizedTimestamp < currentBorders.Past)
            {
                missedPastEvents.Add();
                return;
            }

            if (normalizedTimestamp >= currentBorders.Future)
            {
                missedFutureEvents.Add();
                return;
            }

            var timeBin = timeBins.GetOrAdd(normalizedTimestamp, _ => new TimeBin());
            timeBin.Consume(values);
        }

        public IEnumerable<MetricEvent> Reset(Borders nextBorders)
        {
            nextBorders = NormalizeBorders(nextBorders);
            Interlocked.Exchange(ref borders, nextBorders);
            foreach (var kvp in timeBins)
            {
                var timestamp = kvp.Key;
                var timeBin = kvp.Value;
                if (timestamp < nextBorders.Past)
                {
                    if (timeBins.TryRemove(timestamp, out _))
                        yield return AggregateMetric(timestamp, timeBin);
                }
                else if (timeBin.TryFlush(cooldownPeriod))
                    yield return AggregateMetric(timestamp, timeBin);
            }
        }

        private MetricEvent AggregateMetric(DateTimeOffset timestamp, TimeBin timeBin)
        {
            var values = new Dictionary<string, double>
            {
                {"count", timeBin.Counter.GetValue()}
            };
            foreach (var m in timeBin.Meters)
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

        private Borders NormalizeBorders(Borders b)
        {
            return new Borders(NormalizeTimestamp(b.Past), NormalizeTimestamp(b.Future));
        }

        private DateTimeOffset NormalizeTimestamp(DateTimeOffset timestamp)
        {
            return new DateTimeOffset(timestamp.Ticks - timestamp.Ticks%period.Ticks, TimeSpan.Zero);
        }

        private class TimeBin
        {
            private long lastConsumeTimeUtcTicks;
            private long flushedEvents;

            public ConcurrentDictionary<string, Meter> Meters { get; } = new ConcurrentDictionary<string, Meter>();
            public Counter Counter { get; } = new Counter();

            public bool TryFlush(TimeSpan cooldownPeriod)
            {
                if (GetLastConsumeTime() > DateTimeOffset.UtcNow - cooldownPeriod)
                    return false;
                var eventsCount = Counter.GetValue();
                if (flushedEvents == eventsCount)
                    return false;
                flushedEvents = eventsCount;
                return true;
            }

            public void Consume(IReadOnlyDictionary<string, double> values)
            {
                Interlocked.Exchange(ref lastConsumeTimeUtcTicks, DateTimeOffset.UtcNow.UtcTicks);
                Counter.Add();
                foreach (var kvp in values)
                {
                    var meter = Meters.GetOrAdd(kvp.Key, _ => new Meter());
                    meter.Add(kvp.Value);
                }
            }

            private DateTimeOffset GetLastConsumeTime() => new DateTimeOffset(Interlocked.Read(ref lastConsumeTimeUtcTicks), TimeSpan.Zero);
        }
    }
}
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using Vstk.Metrics;

namespace Vstk.AirlockConsumer.MetricsAggregator
{
    public class Bucket : IBucket
    {
        private readonly AggregatorMetrics aggregatorMetrics;
        private readonly IReadOnlyDictionary<string, string> tags;
        private readonly TimeSpan period;
        private readonly TimeSpan cooldownPeriod;
        private readonly ConcurrentDictionary<DateTimeOffset, TimeBin> timeBins;
        private Borders borders;

        public Bucket(
            AggregatorMetrics aggregatorMetrics,
            IReadOnlyDictionary<string, string> tags,
            TimeSpan period,
            TimeSpan cooldownPeriod,
            Borders borders)
        {
            this.aggregatorMetrics = aggregatorMetrics;
            this.tags = tags;
            this.period = period;
            this.cooldownPeriod = cooldownPeriod;
            timeBins = new ConcurrentDictionary<DateTimeOffset, TimeBin>();
            this.borders = NormalizeBorders(borders);
        }

        public void Consume(IReadOnlyDictionary<string, double> values, DateTimeOffset timestamp)
        {
            var normalizedTimestamp = NormalizeTimestamp(timestamp);
            var currentBorders = Interlocked.CompareExchange(ref borders, null, null);

            if (normalizedTimestamp < currentBorders.Past)
            {
                aggregatorMetrics.MissedPastEvents.Add();
                return;
            }

            if (normalizedTimestamp >= currentBorders.Future)
            {
                aggregatorMetrics.MissedFutureEvents.Add();
                return;
            }

            var timeBin = timeBins.GetOrAdd(normalizedTimestamp, t => new TimeBin(t));
            timeBin.Consume(values);
        }

        public IEnumerable<MetricEvent> Flush(Borders nextBorders)
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
                    {
                        var metric = timeBin.TryFlush(tags);
                        if (metric != null)
                            yield return metric;
                    }
                }
                else
                {
                    var metric = timeBin.TryFlush(tags, cooldownPeriod);
                    if (metric != null)
                        yield return metric;
                }
            }
        }

        private Borders NormalizeBorders(Borders b)
        {
            return new Borders(NormalizeTimestamp(b.Past), NormalizeTimestamp(b.Future));
        }

        private DateTimeOffset NormalizeTimestamp(DateTimeOffset timestamp)
        {
            return new DateTimeOffset(timestamp.Ticks - timestamp.Ticks%period.Ticks, TimeSpan.Zero);
        }
    }
}
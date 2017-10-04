using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Vostok.Metrics;

namespace Vostok.AirlockConsumer.MetricsAggregator
{
    internal class Bucket : IBucket
    {
        private readonly IMeterFactory meterFactory;
        private readonly IReadOnlyDictionary<string, string> tags;
        private readonly TimeSpan period;

        private readonly ConcurrentDictionary<DateTimeOffset, ConcurrentDictionary<string, IMeter>> meters;

        private long borderTicks;

        public Bucket(
            IMeterFactory meterFactory,
            IReadOnlyDictionary<string, string> tags,
            TimeSpan period)
        {
            this.meterFactory = meterFactory;
            this.tags = tags;
            this.period = period;
            meters = new ConcurrentDictionary<DateTimeOffset, ConcurrentDictionary<string, IMeter>>();
        }

        public void Consume(IReadOnlyDictionary<string, double> values, DateTimeOffset timestamp)
        {
            var normalizedTimestamp = NormalizeTimestamp(timestamp);

            if (normalizedTimestamp.Ticks < borderTicks)
            {
                //TODO (@ezsilmar) meter missed event here
                return;
            }

            var byTimestamp = meters.GetOrAdd(normalizedTimestamp, _ => new ConcurrentDictionary<string, IMeter>());
            foreach (var kvp in values)
            {
                var meter = byTimestamp.GetOrAdd(kvp.Key, meterFactory.Create);
                meter.Add(kvp.Value);
            }
        }

        private DateTimeOffset NormalizeTimestamp(DateTimeOffset timestamp)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<MetricEvent> Reset(DateTimeOffset border)
        {
            var normalizedBorder = NormalizeTimestamp(border);
            Interlocked.Exchange(ref borderTicks, normalizedBorder.Ticks);
            foreach (var kvp in meters)
            {
                if (kvp.Key < normalizedBorder)
                {
                    throw new NotImplementedException();
                }
            }
        }
    }
}
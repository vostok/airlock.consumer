using System;
using System.Collections.Generic;

namespace Vostok.AirlockConsumer.MetricsAggregator
{
    internal class EventsTimestampProvider : IEventsTimestampProvider
    {
        private readonly int maxCapacity;
        private readonly Queue<DateTimeOffset> offsets = new Queue<DateTimeOffset>();
        private DateTimeOffset? lastRegisteredTime;

        public EventsTimestampProvider(int maxCapacity)
        {
            this.maxCapacity = maxCapacity;
        }

        public void AddTimestamp(DateTimeOffset timestamp)
        {
            offsets.Enqueue(timestamp);
            if (offsets.Count > maxCapacity)
            {
                offsets.Dequeue();
            }

            lastRegisteredTime = DateTimeOffset.UtcNow;
        }

        public DateTimeOffset? Now()
        {
            if (offsets.Count < maxCapacity)
            {
                return null;
            }

            var items = offsets.ToArray();
            Array.Sort(items);
            return items[items.Length/2];
        }

        public DateTimeOffset? GetLastRegisteredTime()
        {
            return lastRegisteredTime;
        }
    }
}
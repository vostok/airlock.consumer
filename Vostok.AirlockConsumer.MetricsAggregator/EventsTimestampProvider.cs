using System;
using System.Collections.Generic;

namespace Vostok.AirlockConsumer.MetricsAggregator
{
    public class EventsTimestampProvider : IEventsTimestampProvider
    {
        private readonly int maxCapacity;
        private readonly Queue<DateTimeOffset> offsets = new Queue<DateTimeOffset>();

        public EventsTimestampProvider(int maxCapacity)
        {
            this.maxCapacity = maxCapacity;
        }

        public void AddTimestamp(DateTimeOffset timestamp)
        {
            offsets.Enqueue(timestamp);
            if (offsets.Count > maxCapacity)
                offsets.Dequeue();
        }

        public DateTimeOffset? Now()
        {
            if (offsets.Count < maxCapacity)
                return null;

            var items = offsets.ToArray();
            Array.Sort(items);
            return items[items.Length/2];
        }
    }
}
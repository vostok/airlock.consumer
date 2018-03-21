using System;

namespace Vostok.Airlock.Consumer.MetricsAggregator
{
    public interface IEventsTimestampProvider
    {
        void AddTimestamp(DateTimeOffset timestamp);
        DateTimeOffset? Now();
    }
}
using System;

namespace Vostok.AirlockConsumer.MetricsAggregator
{
    public interface IEventsTimestampProvider
    {
        void AddTimestamp(DateTimeOffset timestamp);
        DateTimeOffset? Now();
    }
}
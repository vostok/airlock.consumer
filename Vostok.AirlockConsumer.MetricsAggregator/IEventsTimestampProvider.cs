using System;

namespace Vstk.AirlockConsumer.MetricsAggregator
{
    public interface IEventsTimestampProvider
    {
        void AddTimestamp(DateTimeOffset timestamp);
        DateTimeOffset? Now();
    }
}
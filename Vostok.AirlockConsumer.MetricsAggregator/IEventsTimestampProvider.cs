using System;

namespace Vostok.AirlockConsumer.MetricsAggregator
{
    internal interface IEventsTimestampProvider
    {
        void AddTimestamp(DateTimeOffset timestamp);
        DateTimeOffset? Now();
        DateTimeOffset? GetLastRegisteredTime();
    }
}
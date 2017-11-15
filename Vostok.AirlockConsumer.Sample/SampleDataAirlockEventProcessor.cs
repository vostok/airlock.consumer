﻿using System;
using System.Collections.Generic;
using Vostok.Logging;
using Vostok.Metrics.Meters;

namespace Vostok.AirlockConsumer.Sample
{
    public class SampleDataAirlockEventProcessor : IAirlockEventProcessor<SampleEvent>
    {
        private readonly ILog log;
        private readonly TimeSpan? recedeGap;

        public SampleDataAirlockEventProcessor(ILog log, TimeSpan? recedeGap)
        {
            this.log = log;
            this.recedeGap = recedeGap;
        }

        public DateTimeOffset? GetStartTimestampOnRebalance(string eventsRoutingKey)
        {
            if (!recedeGap.HasValue)
                return null;
            var startTimestampOnRebalance = DateTimeOffset.UtcNow - recedeGap.Value;
            log.Warn($"Going back on {eventsRoutingKey} to timestamp: {startTimestampOnRebalance}");
            return startTimestampOnRebalance;
        }

        public void Process(List<AirlockEvent<SampleEvent>> events, ICounter messageProcessedCounter)
        {
            log.Info($"New events batch has arrived of size {events.Count}");
            foreach (var @event in events)
                log.Info($"{@event.RoutingKey}|{@event.Timestamp:O} - {@event.Payload.Message}");
        }

        public void Release(string routingKey)
        {
            log.Warn($"Release for: {routingKey}");
        }
    }
}
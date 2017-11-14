using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Vostok.Metrics;
using Vostok.Metrics.Meters;

namespace Vostok.AirlockConsumer.MetricsAggregator
{
    // todo (avk, 06.10.2017): we need only one MetricAggregationService per project https://github.com/vostok/airlock.consumer/issues/18
    internal class MetricAirlockEventProcessor : SimpleAirlockEventProcessorBase<MetricEvent>
    {
        private readonly Func<string, MetricAggregationService> serviceFactory;
        private readonly ConcurrentDictionary<string, MetricAggregationService> services;

        public MetricAirlockEventProcessor(Func<string, MetricAggregationService> serviceFactory)
        {
            this.serviceFactory = serviceFactory;
            services = new ConcurrentDictionary<string, MetricAggregationService>();
        }

        public sealed override void Process(List<AirlockEvent<MetricEvent>> events, ICounter messageProcessedCounter)
        {
            foreach (var consumerEvent in events)
            {
                var service = services.GetOrAdd(consumerEvent.RoutingKey, serviceFactory);
                service.Start();
                service.ProcessMetricEvent(consumerEvent.Payload);
                messageProcessedCounter.Add();
            }
        }

        public void Stop()
        {
            foreach (var kvp in services)
                kvp.Value.Stop();
        }
    }
}
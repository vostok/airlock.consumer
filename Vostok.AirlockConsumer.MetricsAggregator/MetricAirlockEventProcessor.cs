using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Vostok.Metrics;

namespace Vostok.AirlockConsumer.MetricsAggregator
{
    internal class MetricAirlockEventProcessor : IAirlockEventProcessor<MetricEvent>
    {
        private readonly Func<string, MetricAggregationService> serviceFactory;
        private readonly ConcurrentDictionary<string, MetricAggregationService> services;

        public MetricAirlockEventProcessor(Func<string, MetricAggregationService> serviceFactory)
        {
            this.serviceFactory = serviceFactory;
            services = new ConcurrentDictionary<string, MetricAggregationService>();
        }

        public void Process(List<AirlockEvent<MetricEvent>> events)
        {
            foreach (var consumerEvent in events)
            {
                var service = services.GetOrAdd(consumerEvent.RoutingKey, serviceFactory);
                service.Start();
                service.ProcessMetricEvent(consumerEvent.Payload);
            }
        }

        public void Stop()
        {
            foreach (var kvp in services)
                kvp.Value.Stop();
        }
    }
}
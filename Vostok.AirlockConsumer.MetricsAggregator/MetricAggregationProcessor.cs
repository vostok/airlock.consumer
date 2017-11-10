using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Vostok.Metrics;
using Vostok.Metrics.Meters;

namespace Vostok.AirlockConsumer.MetricsAggregator
{
    internal class MetricAggregationProcessor : SimpleAirlockEventProcessorBase<MetricEvent>
    {
        private readonly Func<string, MetricAggregationService> serviceFactory;
        private readonly ConcurrentDictionary<string, MetricAggregationService> services = new ConcurrentDictionary<string, MetricAggregationService>();

        public MetricAggregationProcessor(Func<string, MetricAggregationService> serviceFactory)
        {
            this.serviceFactory = serviceFactory;
        }

        public sealed override void Process(List<AirlockEvent<MetricEvent>> events, ICounter messageProcessedCounter)
        {
            foreach (var consumerEvent in events)
            {
                var service = services.GetOrAdd(consumerEvent.RoutingKey, serviceFactory);
                service.ProcessMetricEvent(consumerEvent.Payload);
                messageProcessedCounter.Add();
            }
        }

        public override void Release(string routingKey)
        {
            foreach (var kvp in services)
                kvp.Value.Stop();
        }
    }
}
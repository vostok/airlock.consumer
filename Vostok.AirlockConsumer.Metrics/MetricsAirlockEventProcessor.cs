using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Vostok.Graphite.Client;
using Vostok.Logging;
using Vostok.Metrics;
using Vostok.Metrics.Meters;
using Vostok.RetriableCall;

namespace Vostok.AirlockConsumer.Metrics
{
    public class MetricsAirlockEventProcessor : SimpleAirlockEventProcessorBase<MetricEvent>
    {
        private readonly ILog log;
        private readonly MetricConverter metricConverter;
        private readonly GraphiteClient graphiteClient;
        private readonly RetriableCallStrategy retriableCallStrategy = new RetriableCallStrategy(3, TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(100));

        public MetricsAirlockEventProcessor(Uri graphiteUri, ILog log)
        {
            this.log = log;
            var graphiteNameBuidler = new GraphiteNameBuilder();
            metricConverter = new MetricConverter(graphiteNameBuidler);
            graphiteClient = new GraphiteClient(graphiteUri.Host, graphiteUri.Port);
        }

        public sealed override void Process(List<AirlockEvent<MetricEvent>> events, ICounter messageProcessedCounter)
        {
            var metrics = events.SelectMany(x => metricConverter.Convert(x.RoutingKey, x.Payload));
            SendBatchAsync(metrics.ToArray()).GetAwaiter().GetResult();
                messageProcessedCounter.Add(events.Count);
            }
        }

        private async Task SendBatchAsync(IReadOnlyCollection<Metric> batchMetrics)
        {
            await retriableCallStrategy.CallAsync(() => graphiteClient.SendAsync(batchMetrics), ex => true, log);
        }
    }
}
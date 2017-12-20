using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Vostok.Graphite.Client;
using Vostok.Logging;
using Vostok.Metrics;
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

        public sealed override void Process(List<AirlockEvent<MetricEvent>> events, ProcessorMetrics processorMetrics)
        {
            try
            {
                var metrics = events.SelectMany(x => metricConverter.Convert(x.RoutingKey, x.Payload));
                SendBatchAsync(metrics.ToArray(), processorMetrics).GetAwaiter().GetResult();
                processorMetrics.MessageProcessedCounter.Add(events.Count);
            }
            catch (Exception)
            {
                processorMetrics.MessageFailedCounter.Add(events.Count);
                throw;
            }
        }

        private async Task SendBatchAsync(IReadOnlyCollection<Metric> events, ProcessorMetrics processorMetrics)
        {
            await retriableCallStrategy.CallAsync(() => graphiteClient.SendAsync(events), ex =>
            {
                processorMetrics.SendingErrorCounter.Add();
                return true;
            }, log);
        }
    }
}
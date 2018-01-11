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
            graphiteClient = new GraphiteClient(graphiteUri.Host, graphiteUri.Port, log);
        }

        public sealed override void Process(List<AirlockEvent<MetricEvent>> events, ProcessorMetrics processorMetrics)
        {
            var metrics = events.SelectMany(x => metricConverter.Convert(x.RoutingKey, x.Payload)).ToArray();
            try
            {
                SendBatchAsync(metrics, processorMetrics).GetAwaiter().GetResult();
                processorMetrics.EventProcessedCounter.Add(events.Count);
            }
            catch (Exception)
            {
                processorMetrics.EventFailedCounter.Add(events.Count);
                throw;
            }
        }

        private async Task SendBatchAsync(Metric[] metrics, ProcessorMetrics processorMetrics)
        {
            await retriableCallStrategy.CallAsync(() => graphiteClient.SendAsync(metrics), ex =>
            {
                processorMetrics.SendingErrorCounter.Add();
                return true;
            }, log);
        }
    }
}
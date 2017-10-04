using System.Collections.Generic;
using System.Linq;
using Vostok.Metrics;

namespace Vostok.AirlockConsumer.FinalMetrics
{
    internal class FinalMetricsMessageProcessor : IMessageProcessor<MetricEvent>
    {
        private readonly MetricConverter metricConverter;
        private readonly GraphiteClient.GraphiteClient graphiteClient;

        public FinalMetricsMessageProcessor(string graphiteHost, int graphitePort)
        {
            var graphiteNameBuidler = new GraphiteNameBuilder();
            metricConverter = new MetricConverter(graphiteNameBuidler);
            graphiteClient = new GraphiteClient.GraphiteClient(graphiteHost, graphitePort);
        }

        public void Process(List<AirlockEvent<MetricEvent>> events)
        {
            var metrics = events.SelectMany(x => metricConverter.Convert(x.RoutingKey, x.Payload));
            graphiteClient.SendAsync(metrics).GetAwaiter().GetResult();
        }
    }
}
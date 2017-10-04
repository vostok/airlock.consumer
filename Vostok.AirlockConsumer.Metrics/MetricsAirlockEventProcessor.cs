using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Vostok.Graphite.Client;
using Vostok.Metrics;

namespace Vostok.AirlockConsumer.Metrics
{
    public class MetricsAirlockEventProcessor : IAirlockEventProcessor<MetricEvent>
    {
        private readonly MetricConverter metricConverter;
        private readonly GraphiteClient graphiteClient;

        public MetricsAirlockEventProcessor(string graphiteHost, int graphitePort)
        {
            var graphiteNameBuidler = new GraphiteNameBuilder();
            metricConverter = new MetricConverter(graphiteNameBuidler);
            graphiteClient = new GraphiteClient(graphiteHost, graphitePort);
        }

        public async Task ProcessAsync(List<AirlockEvent<MetricEvent>> events)
        {
            var metrics = events.SelectMany(x => metricConverter.Convert(x.RoutingKey, x.Payload));
            await graphiteClient.SendAsync(metrics).ConfigureAwait(false);
        }
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Vostok.Commons.Extensions.UnitConvertions;
using Vostok.Graphite.Client;
using Vostok.Logging;
using Vostok.Metrics;

namespace Vostok.AirlockConsumer.Metrics
{
    public class MetricsAirlockEventProcessor : SimpleAirlockEventProcessorBase<MetricEvent>
    {
        private readonly ILog log;
        private readonly MetricConverter metricConverter;
        private readonly GraphiteClient graphiteClient;
        private readonly TimeSpan sendPeriod = 10.Seconds();
        private const int attemptCount = 3;

        public MetricsAirlockEventProcessor(Uri graphiteUri, ILog log)
        {
            this.log = log;
            var graphiteNameBuidler = new GraphiteNameBuilder();
            metricConverter = new MetricConverter(graphiteNameBuidler);
            graphiteClient = new GraphiteClient(graphiteUri.Host, graphiteUri.Port);
        }

        public sealed override void Process(List<AirlockEvent<MetricEvent>> events)
        {
            var metrics = events.SelectMany(x => metricConverter.Convert(x.RoutingKey, x.Payload));
            SendBatchAsync(metrics.ToArray()).GetAwaiter().GetResult();
        }

        // todo (avk, 05.10.2017): simplify processors https://github.com/vostok/airlock.consumer/issues/16
        private async Task SendBatchAsync(IReadOnlyCollection<Metric> batchMetrics)
        {
            var attemptTimeout = TimeSpan.Zero;
            var attempt = 1;
            while (true)
            {
                try
                {
                    await graphiteClient.SendAsync(batchMetrics).ConfigureAwait(false);
                    return;
                }
                catch (Exception e)
                {
                    if (attempt > attemptCount)
                        throw;

                    log.Warn(e);
                }

                attempt++;
                if (attemptTimeout == TimeSpan.Zero)
                {
                    attemptTimeout = sendPeriod;
                }
                else
                {
                    await Task.Delay(attemptTimeout).ConfigureAwait(false);
                    attemptTimeout *= 2;
                }
            }
        }
    }
}
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
        private readonly ThrowableLazy<GraphiteClient> graphiteClient;
        private readonly TimeSpan sendPeriod = 10.Seconds();
        private const int batchSize = 10000;
        private const int attemptCount = 3;

        public MetricsAirlockEventProcessor(Uri graphiteUri, ILog log)
        {
            this.log = log;
            var graphiteNameBuidler = new GraphiteNameBuilder();
            metricConverter = new MetricConverter(graphiteNameBuidler);
            graphiteClient = new ThrowableLazy<GraphiteClient>(() => new GraphiteClient(graphiteUri.Host, graphiteUri.Port), log);
        }

        public sealed override void Process(List<AirlockEvent<MetricEvent>> events)
        {
            log.Info("Start process metrics");
            var metrics = events.SelectMany(x => metricConverter.Convert(x.RoutingKey, x.Payload));

            foreach (var batch in Split(metrics, batchSize))
            {
                SendBatchAsync(batch, attemptCount, sendPeriod).GetAwaiter().GetResult();
            }
            log.Info("Finished process metrics");
        }

        // todo (avk, 05.10.2017): simplify processors https://github.com/vostok/airlock.consumer/issues/16
        private async Task SendBatchAsync(List<Metric> batchMetrics, int attemptCount, TimeSpan sendPeriod)
        {
            var attemptTimeout = TimeSpan.Zero;
            var attempt = 1;
            while (true)
            {
                try
                {
                    await graphiteClient.Value.SendAsync(batchMetrics).ConfigureAwait(false);
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

        private static IEnumerable<List<T>> Split<T>(IEnumerable<T> source, int batchSize)
        {
            var batch = new List<T>(batchSize);
            foreach (var item in source)
            {
                if (batch.Count >= batchSize)
                {
                    yield return batch;
                    batch = new List<T>(batchSize);
                }
                batch.Add(item);
            }

            if (batch.Count > 0)
            {
                yield return batch;
            }
        }
    }
}
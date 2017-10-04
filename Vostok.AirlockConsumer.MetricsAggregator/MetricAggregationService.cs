using System.Threading;
using System.Threading.Tasks;
using Vostok.Metrics;

namespace Vostok.AirlockConsumer.MetricsAggregator
{
    internal class MetricAggregationService
    {
        private readonly IMetricAggregator aggregator;
        private readonly IEventsTimestampProvider eventsTimestampProvider;
        private readonly MetricResetDaemon resetDaemon;
        private readonly Borders initialBorders;

        private Task resetDaemonTask;

        private int isStarted;

        public MetricAggregationService(
            IMetricAggregator aggregator,
            IEventsTimestampProvider eventsTimestampProvider,
            MetricResetDaemon resetDaemon,
            Borders initialBorders)
        {
            this.aggregator = aggregator;
            this.eventsTimestampProvider = eventsTimestampProvider;
            this.resetDaemon = resetDaemon;
            this.initialBorders = initialBorders;
        }

        public void ProcessMetricEvent(MetricEvent metricEvent)
        {
            eventsTimestampProvider.AddTimestamp(metricEvent.Timestamp);
            aggregator.ProcessMetricEvent(metricEvent);
        }

        public void Start()
        {
            if (Interlocked.CompareExchange(ref isStarted, 1, 0) != 0)
            {
                return;
            }
            resetDaemonTask = resetDaemon.StartAsync(initialBorders);
        }

        public void Stop()
        {
            if (Interlocked.CompareExchange(ref isStarted, 0, 1) != 1)
            {
                return;
            }
            resetDaemon.Stop();
            resetDaemonTask.GetAwaiter().GetResult();
        }
    }
}
using System.Threading.Tasks;
using Vostok.Metrics;

namespace Vostok.AirlockConsumer.MetricsAggregator
{
    internal class MetricAggregationService
    {
        private readonly IMetricAggregator aggregator;
        private readonly IEventsTimestampProvider eventsTimestampProvider;
        private readonly MetricResetDaemon resetDaemon;

        private readonly Task resetDaemonTask;

        public MetricAggregationService(
            IMetricAggregator aggregator,
            IEventsTimestampProvider eventsTimestampProvider,
            MetricResetDaemon resetDaemon)
        {
            this.aggregator = aggregator;
            this.eventsTimestampProvider = eventsTimestampProvider;
            this.resetDaemon = resetDaemon;
            resetDaemonTask = resetDaemon.StartAsync();
        }

        public void ProcessMetricEvent(MetricEvent metricEvent)
        {
            eventsTimestampProvider.AddTimestamp(metricEvent.Timestamp);
            aggregator.ProcessMetricEvent(metricEvent);
        }

        public void Stop()
        {
            resetDaemon.Stop();
            resetDaemonTask.GetAwaiter().GetResult();
        }
    }
}
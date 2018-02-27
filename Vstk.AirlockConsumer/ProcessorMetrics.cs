using System;
using Vstk.Metrics;
using Vstk.Metrics.Meters;

namespace Vstk.AirlockConsumer
{
    public class ProcessorMetrics : IDisposable
    {
        public ICounter EventProcessedCounter { get; }
        public ICounter EventIgnoredCounter { get; }
        public ICounter EventFailedCounter { get; }
        public ICounter SendingErrorCounter { get; }

        public ProcessorMetrics(IMetricScope metricScope, TimeSpan flushMetricsInterval)
        {
            EventProcessedCounter = metricScope.Counter(flushMetricsInterval, "event_processed");
            EventIgnoredCounter = metricScope.Counter(flushMetricsInterval, "event_ignored");
            EventFailedCounter = metricScope.Counter(flushMetricsInterval, "event_failed");
            SendingErrorCounter = metricScope.Counter(flushMetricsInterval, "sending_errors");
        }

        public void Dispose()
        {
            (EventProcessedCounter as IDisposable)?.Dispose();
            (EventIgnoredCounter as IDisposable)?.Dispose();
            (SendingErrorCounter as IDisposable)?.Dispose();
            (EventFailedCounter as IDisposable)?.Dispose();
        }
    }
}
using System;
using Vostok.Metrics;
using Vostok.Metrics.Meters;

namespace Vostok.AirlockConsumer
{
    public class ProcessorMetrics : IDisposable
    {
        public ICounter MessageProcessedCounter { get; }
        public ICounter MessageIgnoredCounter { get; }
        public ICounter SendingErrorCounter { get; }
        public ICounter MessageFailedCounter { get; }

        public ProcessorMetrics(IMetricScope metricScope, TimeSpan flushMetricsInterval)
        {
            MessageProcessedCounter = metricScope.Counter(flushMetricsInterval, "message_processed");
            MessageIgnoredCounter = metricScope.Counter(flushMetricsInterval, "message_ignored");
            SendingErrorCounter = metricScope.Counter(flushMetricsInterval, "sending_errors");
            MessageFailedCounter = metricScope.Counter(flushMetricsInterval, "message_failed");
        }

        public void Dispose()
        {
            (MessageProcessedCounter as IDisposable)?.Dispose();
            (MessageIgnoredCounter as IDisposable)?.Dispose();
            (SendingErrorCounter as IDisposable)?.Dispose();
            (MessageFailedCounter as IDisposable)?.Dispose();
        }
    }
}
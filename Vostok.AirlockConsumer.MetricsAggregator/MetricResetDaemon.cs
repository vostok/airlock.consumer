using System;
using System.Threading;
using System.Threading.Tasks;

namespace Vostok.AirlockConsumer.MetricsAggregator
{
    public class MetricResetDaemon
    {
        private readonly IEventsTimestampProvider eventsTimestampProvider;
        private readonly MetricsAggregatorSettings settings;
        private readonly MetricAggregator aggregator;
        private readonly CancellationTokenSource cts;

        public MetricResetDaemon(
            IEventsTimestampProvider eventsTimestampProvider,
            MetricsAggregatorSettings settings,
            MetricAggregator aggregator)
        {
            this.eventsTimestampProvider = eventsTimestampProvider;
            this.settings = settings;
            this.aggregator = aggregator;
            cts = new CancellationTokenSource();
        }

        public async Task StartAsync(Borders currentBorders)
        {
            while (!cts.Token.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(settings.MetricResetDaemonIterationPeriod, cts.Token);
                }
                catch (OperationCanceledException)
                {
                    return;
                }

                currentBorders = CalculateNewBorders(currentBorders);
                aggregator.Flush(currentBorders);
            }
        }

        public void Stop()
        {
            cts.Cancel();
        }

        private Borders CalculateNewBorders(Borders current)
        {
            var now = DateTimeOffset.UtcNow;

            var future = now + settings.MetricAggregationFutureGap;
            var past = eventsTimestampProvider.Now() ?? current.Past;
            if (past < current.Past)
                past = current.Past;
            var maxPossiblePast = now - settings.MetricAggregationPastGap;
            if (past > maxPossiblePast)
                past = maxPossiblePast;

            return new Borders(past, future);
        }
    }
}
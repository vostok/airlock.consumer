using System;
using System.Threading;
using System.Threading.Tasks;

namespace Vostok.AirlockConsumer.MetricsAggregator
{
    internal class MetricResetDaemon
    {
        private readonly IEventsTimestampProvider eventsTimestampProvider;
        private readonly MetricsAggregatorSettings settings;
        private readonly IMetricAggregator aggregator;

        private readonly CancellationTokenSource cts;

        public MetricResetDaemon(
            IEventsTimestampProvider eventsTimestampProvider,
            MetricsAggregatorSettings settings,
            IMetricAggregator aggregator)
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
                    await Task.Delay(5000, cts.Token);
                }
                catch (OperationCanceledException)
                {
                    return;
                }

                currentBorders = CalculateNewBorders(currentBorders);
                aggregator.Reset(currentBorders);
            }
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

        public void Stop()
        {
            cts.Cancel();
        }
    }
}
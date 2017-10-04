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

                var lastRegisteredTime = eventsTimestampProvider.GetLastRegisteredTime();
                if (lastRegisteredTime.HasValue && lastRegisteredTime < DateTimeOffset.UtcNow - settings.MetricAggregationPastGap)
                {
                    aggregator.Flush();
                }
            }
        }

        private Borders CalculateNewBorders(Borders current)
        {
            var newFuture = DateTimeOffset.UtcNow + settings.MetricAggregationFutureGap;

            var eventsNow = eventsTimestampProvider.Now();
            if (eventsNow == null)
                return new Borders(current.Past, newFuture);

            var newPast = Max(eventsNow.Value - settings.MetricAggregationPastGap, current.Past);
            return new Borders(newPast, newFuture);
        }

        private static DateTimeOffset Max(DateTimeOffset a, DateTimeOffset b)
        {
            return a > b ? a : b;
        }

        public void Stop()
        {
            cts.Cancel();
        }
    }
}
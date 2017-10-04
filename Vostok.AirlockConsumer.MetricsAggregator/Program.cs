using System;
using Serilog;
using Vostok.Airlock;
using Vostok.Commons.Extensions.UnitConvertions;
using Vostok.Logging;
using Vostok.Logging.Serilog;
using Vostok.Metrics;

namespace Vostok.AirlockConsumer.MetricsAggregator
{
    class Program
    {
        public static ILog Log;

        static void Main(string[] args)
        {
            var logger = new LoggerConfiguration()
                .WriteTo.Console()
                .WriteTo.RollingFile("..\\log\\actions-{Date}.txt")
                .MinimumLevel.Debug()
                .CreateLogger();
            Log = new SerilogLog(logger);
            var settings = Util.ReadYamlSettings<MetricsAggregatorSettings>("metricsAggregator.yaml");
            //TODO create IAirlock here
            var metricAggregator = new MetricAggregator(new BucketKeyProvider(), null, CreateBorders(DateTimeOffset.UtcNow, settings));
            var processor = new MetricEventProcessor(metricAggregator);
            var consumer = new MetricEventConsumer(settings, processor);
            var clock = new MetricClock(1.Minutes());
            clock.Register(
                timestamp => metricAggregator.Reset(CreateBorders(timestamp, settings)));
            clock.Start();
            consumer.Start();
            Console.ReadLine();
            consumer.Stop();
            clock.Stop();
        }

        private static Borders CreateBorders(DateTimeOffset timestamp, MetricsAggregatorSettings settings)
        {
            return new Borders(timestamp - settings.MetricAggregationPastGap, timestamp + settings.MetricAggregationFutureGap);
        }
    }
}

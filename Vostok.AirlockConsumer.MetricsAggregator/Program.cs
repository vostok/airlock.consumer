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
            var metricAggregator = new MetricAggregator(new BucketKeyProvider(), null);
            var processor = new MetricEventProcessor(metricAggregator);
            var consumer = new MetricEventConsumer(settings, processor);
            var clock = new MetricClock(1.Minutes());
            clock.Register(
                timestamp => metricAggregator.Reset(timestamp - settings.MetricAggregationGap));
            clock.Start();
            consumer.Start();
            Console.ReadLine();
            consumer.Stop();
            clock.Stop();
        }
    }
}

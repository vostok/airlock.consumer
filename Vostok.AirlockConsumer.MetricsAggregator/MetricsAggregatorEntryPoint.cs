using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using Serilog;
using Vostok.Commons.Extensions.UnitConvertions;
using Vostok.Logging.Serilog;
using Vostok.Metrics;

namespace Vostok.AirlockConsumer.MetricsAggregator
{
    internal class MetricsAggregatorEntryPoint
    {
        private static void Main(string[] args)
        {
            var settings = Util.ReadYamlSettings<Dictionary<string, object>>(GetSettingsFileName(args));
            settings["client.id"] = Dns.GetHostName();

            var aggregatorSettings = new MetricsAggregatorSettings
            {
                MetricAggregationPastGap = ParseTimeSpan(settings["airlock.metricsAggregator.pastGap"], 20.Seconds()),
                MetricAggregationFutureGap = ParseTimeSpan(settings["airlock.metricsAggregator.pastGap"], 1.Hours())
            };

            var logger = new LoggerConfiguration()
                .WriteTo.Console()
                .WriteTo.RollingFile((string) settings["airlock.consumer.log.file.pattern"])
                .MinimumLevel.Debug()
                .CreateLogger();
            var log = new SerilogLog(logger);
            //TODO create IAirlock here
            var rootMetricScope = new RootMetricScope(new MetricConfiguration());
            var metricAggregator = new MetricAggregator(rootMetricScope, new BucketKeyProvider(), null, CreateBorders(DateTimeOffset.UtcNow, aggregatorSettings));
            var processor = new MetricEventProcessor(metricAggregator);
            var consumer = new MetricEventConsumer(settings, processor, log);
            var clock = new MetricClock(1.Minutes());
            clock.Register(
                timestamp => metricAggregator.Reset(CreateBorders(timestamp, aggregatorSettings)));
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

        private static string GetSettingsFileName(string[] args)
        {
            return args.Any() ? args[0] : "default-settings.yaml";
        }

        private static TimeSpan ParseTimeSpan(object value, TimeSpan defaultValue)
        {
            return value == null ? defaultValue : TimeSpan.Parse(value.ToString());
        }
    }
}
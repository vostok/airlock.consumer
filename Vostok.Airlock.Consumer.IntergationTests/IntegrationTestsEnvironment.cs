using System;
using System.Diagnostics;
using System.Linq;
using FluentAssertions;
using Vostok.Clusterclient.Topology;
using Vostok.Commons.Extensions.UnitConvertions;
using Vostok.Logging;
using Vostok.Logging.Logs;
using Vostok.Metrics;

namespace Vostok.Airlock.Consumer.IntergationTests
{
    public static class IntegrationTestsEnvironment
    {
        public const string Project = "vostok";
        public const string Environment = "ci";
        private const string airlockGateEndpoints = "http://localhost:6306";

        public static readonly ConsoleLog Log = new ConsoleLog();

        public static void PushToAirlock<T>(string routingKey, T[] events, Func<T, DateTimeOffset> getTimestamp)
        {
            Log.Debug($"Pushing {events.Length} events to airlock");
            var sw = Stopwatch.StartNew();
            AirlockClient airlockClient;
            using (airlockClient = CreateAirlockClient())
            {
                foreach (var @event in events)
                    airlockClient.Push(routingKey, @event, getTimestamp(@event));
            }

            var airlockClientCounters = airlockClient.Counters;
            Log.Debug($"SentItemsCount: {airlockClientCounters.SentItems.GetValue()}, LostItemsCount: {airlockClientCounters.LostItems.GetValue()}, Elapsed: {sw.Elapsed}");
            airlockClientCounters.LostItems.GetValue().Should().Be(0);
            airlockClientCounters.SentItems.GetValue().Should().Be(events.Length);
            MetricClocks.Stop();
        }

        private static AirlockClient CreateAirlockClient()
        {
            var airlockConfig = GetAirlockConfig();
            return new AirlockClient(airlockConfig, Log.FilterByLevel(LogLevel.Warn));
        }

        private static AirlockConfig GetAirlockConfig()
        {
            var airlockGateUris = airlockGateEndpoints.Split(new[] {';'}, StringSplitOptions.RemoveEmptyEntries).Select(x => new Uri(x)).ToArray();
            var airlockConfig = new AirlockConfig
            {
                ApiKey = "UniversalApiKey",
                ClusterProvider = new FixedClusterProvider(airlockGateUris),
                SendPeriod = TimeSpan.FromSeconds(2),
                SendPeriodCap = TimeSpan.FromMinutes(5),
                RequestTimeout = TimeSpan.FromSeconds(30),
                MaximumRecordSize = 1.Kilobytes(),
                MaximumBatchSizeToSend = 300.Megabytes(),
                MaximumMemoryConsumption = 300.Megabytes(),
                InitialPooledBufferSize = 1.Megabytes(),
                InitialPooledBuffersCount = 10,
                EnableTracing = false,
                Parallelism = 10,
                EnableMetrics = false
            };
            Log.Debug($"AirlockConfig: {airlockConfig.ToPrettyJson()}");
            return airlockConfig;
        }
    }
}
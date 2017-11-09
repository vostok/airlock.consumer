using System;
using System.Collections.Generic;
using System.Linq;
using Vostok.Airlock;
using Vostok.Clusterclient.Topology;
using Vostok.Commons.Extensions.UnitConvertions;
using Vostok.Logging;

namespace Vostok.AirlockConsumer.IntergationTests
{
    public static class TestAirlockClientFactory
    {
        private const string defaultAirlockGateEndpoints = "http://gate:6306";
        private const string defaultAirlockGateApiKey = "UniversalApiKey";

        public static IAirlockClient CreateAirlockClient(ILog log, Dictionary<string, string> environmentVariables)
        {
            var airlockConfig = GetAirlockConfig(log, environmentVariables);
            return new ParallelAirlockClient(airlockConfig, 20, log);
        }

        public static AirlockConfig GetAirlockConfig(ILog log, Dictionary<string, string> environmentVariables)
        {
            if (!environmentVariables.TryGetValue("AIRLOCK_GATE_API_KEY", out var airlockGateApiKey))
                airlockGateApiKey = defaultAirlockGateApiKey;
            if (!environmentVariables.TryGetValue("AIRLOCK_GATE_ENDPOINTS", out var airlockGateEndpoints))
                airlockGateEndpoints = defaultAirlockGateEndpoints;
            var airlockGateUris = airlockGateEndpoints.Split(new[] {';'}, StringSplitOptions.RemoveEmptyEntries).Select(x => new Uri(x)).ToArray();
            var airlockConfig = new AirlockConfig
            {
                ApiKey = airlockGateApiKey,
                ClusterProvider = new FixedClusterProvider(airlockGateUris),
                SendPeriod = TimeSpan.FromMilliseconds(10),
                MaximumBatchSizeToSend = 300.Megabytes(),
                MaximumMemoryConsumption = 1.Gigabytes(),
                InitialPooledBufferSize = 10.Megabytes(),
                InitialPooledBuffersCount = 100
            };
            log.Info($"AirlockConfig: {airlockConfig.ToPrettyJson()}");
            return airlockConfig;
        }
    }
}
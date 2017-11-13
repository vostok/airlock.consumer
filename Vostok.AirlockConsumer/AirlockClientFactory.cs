using System;
using System.Collections.Generic;
using System.Linq;
using Vostok.Airlock;
using Vostok.Clusterclient.Topology;
using Vostok.Logging;

namespace Vostok.AirlockConsumer
{
    public static class AirlockClientFactory
    {
        private const string defaultAirlockGateEndpoints = "http://gate:6306";
        private const string defaultAirlockGateApiKey = "UniversalApiKey";

        public static IAirlockClient CreateAirlockClient(Dictionary<string, string> environmentVariables)
        {
            var log = Logging.Configure("./log/airlock-{Date}.log", false);
            var airlockConfig = GetAirlockConfig(log, environmentVariables);
            return new AirlockClient(airlockConfig, log);
        }

        private static AirlockConfig GetAirlockConfig(ILog log, Dictionary<string, string> environmentVariables)
        {
            if (!environmentVariables.TryGetValue("AIRLOCK_GATE_API_KEY", out var airlockGateApiKey))
                airlockGateApiKey = defaultAirlockGateApiKey;
            if (!environmentVariables.TryGetValue("AIRLOCK_GATE_ENDPOINTS", out var airlockGateEndpoints))
                airlockGateEndpoints = defaultAirlockGateEndpoints;
            var airlockGateUris = airlockGateEndpoints.Split(new[] { ';' }, StringSplitOptions.RemoveEmptyEntries).Select(x => new Uri(x)).ToArray();
            var airlockConfig = new AirlockConfig
            {
                ApiKey = airlockGateApiKey,
                ClusterProvider = new FixedClusterProvider(airlockGateUris),
            };
            log.Info($"AirlockConfig: {airlockConfig.ToPrettyJson()}");
            return airlockConfig;
        }
    }
}
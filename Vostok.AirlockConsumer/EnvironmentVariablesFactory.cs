using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Vostok.Logging;

namespace Vostok.AirlockConsumer
{
    public static class EnvironmentVariablesFactory
    {
        public static Dictionary<string, string> GetEnvironmentVariables(ILog log)
        {
            var environmentVariables = Environment.GetEnvironmentVariables(EnvironmentVariableTarget.Process)
                .Cast<DictionaryEntry>()
                .Where(x => ((string) x.Key).StartsWith("AIRLOCK_"))
                .ToDictionary(x => (string) x.Key, x => (string) x.Value);
            log.Info($"EnvironmentVariables: {environmentVariables.ToPrettyJson()}");
            return environmentVariables;
        }
    }
}
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Vostok.Logging;

namespace Vostok.AirlockConsumer
{
    public class AirlockEnvironmentVariables
    {
        public static AirlockEnvironmentVariables CreateFromProcessEnvironment(ILog log)
        {
            var environmentVariables = Environment
                .GetEnvironmentVariables(EnvironmentVariableTarget.Process)
                .Cast<DictionaryEntry>()
                .Where(x => ((string) x.Key).StartsWith("AIRLOCK_"))
                .ToDictionary(x => (string) x.Key, x => (string) x.Value);
            log.Info($"EnvironmentVariables: {environmentVariables.ToPrettyJson()}");
            return new AirlockEnvironmentVariables(environmentVariables);
        }

        private readonly Dictionary<string, string> environmentVariables;

        public AirlockEnvironmentVariables(Dictionary<string, string> environmentVariables)
        {
            this.environmentVariables = environmentVariables;
        }

        public string GetValue(string name, string defaultValue)
        {
            if (!environmentVariables.TryGetValue($"AIRLOCK_{name}", out var value))
                value = defaultValue;
            return value;
        }

        public int GetIntValue(string name, int defaultValue)
        {
            var strValue = GetValue(name, defaultValue.ToString());
            try
            {
                var value = int.Parse(strValue);
                return value;
            }
            catch (Exception e)
            {
                throw new InvalidDataException($"invalid int value for '{name}' setting: '{strValue}'", e);
            }
        }

        public TimeSpan GetTimespan(string name, TimeSpan defaultValue)
        {
            var strValue = GetValue(name, defaultValue.ToString());
            try
            {
                var value = TimeSpan.Parse(strValue);
                return value;
            }
            catch (Exception e)
            {
                throw new InvalidDataException($"invalid timespan value for '{name}' setting: '{strValue}'", e);
            }
        }
    }
}
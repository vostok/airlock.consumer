using System;
using System.Collections;
using System.Collections.Generic;
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
            if (!int.TryParse(strValue, out var value))
                throw new InvalidOperationException($"Invalid int value for {name} variable: {strValue}");
            return value;
        }

        public TimeSpan GetTimeSpanValue(string name, TimeSpan defaultValue)
        {
            var strValue = GetValue(name, defaultValue.ToString());
            if (!TimeSpan.TryParse(strValue, out var value))
                throw new InvalidOperationException($"Invalid TimeSpan value for {name} variable: {strValue}");
            return value;
        }

        public T GetEnumValue<T>(string name, T defaultValue) where T : struct
        {
            var strValue = GetValue(name, defaultValue.ToString());
            if (!Enum.TryParse(strValue, out T value))
                throw new InvalidOperationException($"Invalid {typeof(T).Name} enum value for {name} variable: {strValue}");
            return value;
        }
    }
}
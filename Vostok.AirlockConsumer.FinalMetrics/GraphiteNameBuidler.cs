using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Vostok.AirlockConsumer.FinalMetrics
{
    internal class GraphiteNameBuilder : IGraphiteNameBuilder
    {
        private const string Separator = ".";
        private readonly Dictionary<string, string> nameByRoutingKey = new Dictionary<string, string>();

        private readonly Dictionary<string, TagInfo> defaultTagInfos = new[]
        {
            new TagInfo("Type", 4, null),
            new TagInfo("Host", 3, s => s.ToLower()),
            new TagInfo("OperationName", 2, null),
            new TagInfo("StatusCode", 1, null)
        }.ToDictionary(x => x.Name, x => x, StringComparer.OrdinalIgnoreCase);

        public string Build(string routingKey, IEnumerable<KeyValuePair<string, string>> tags)
        {
            var nameBySystemInfo = GetNameByRoutingKey(routingKey);
            var nameByTags = Build(tags);

            return string.IsNullOrEmpty(nameByTags) ? nameBySystemInfo : nameBySystemInfo + Separator + nameByTags;
        }

        public string Build(string prefix, string name)
        {
            return prefix + Separator + Build(name);
        }

        private string GetNameByRoutingKey(string routingKey)
        {
            if (nameByRoutingKey.ContainsKey(routingKey))
            {
                return nameByRoutingKey[routingKey];
            }

            var systemInfo = SystemInfo.Parse(routingKey);
            var name = Build(systemInfo.Project) + Separator
                       + Build(systemInfo.Environment) + Separator
                       + Build(systemInfo.ServiceName);

            nameByRoutingKey.Add(routingKey, name);
            return name;
        }

        private string Build(IEnumerable<KeyValuePair<string, string>> tags)
        {
            var parts = tags
                .Where(x => !string.IsNullOrEmpty(x.Key))
                .Where(x => !string.IsNullOrEmpty(x.Value))
                .Select(x => new
                {
                    x.Key,
                    x.Value,
                    TagInfo = defaultTagInfos.ContainsKey(x.Key) ? defaultTagInfos[x.Key] : null
                })
                .Select(x => new
                {
                    Key = x.Key.ToLower(),
                    Value = x.TagInfo?.ConvertValue != null ? x.TagInfo.ConvertValue(x.Value) : x.Value,
                    Priority = x.TagInfo?.Priority ?? int.MinValue
                })
                .OrderByDescending(x => x.Priority)
                .ThenBy(x => x.Key)
                .Select(x => Build(x.Value));

            return string.Join(Separator, parts);
        }

        private static string Build(string name)
        {
            if (name.All(IsPermittedSymbol))
            {
                return name;
            }

            var stringBuilder = new StringBuilder(name.Length);
            foreach (var c in name)
            {
                stringBuilder.Append(IsPermittedSymbol(c) ? c : '_');
            }

            return stringBuilder.ToString();
        }

        private static bool IsPermittedSymbol(char c)
        {
            return c >= 'a' && c <= 'z'
                || c >= 'A' && c <= 'Z'
                || c >= '0' && c <= '9';
        }
    }
}
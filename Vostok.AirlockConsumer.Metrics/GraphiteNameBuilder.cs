using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Vostok.Metrics;

namespace Vostok.AirlockConsumer.Metrics
{
    public class GraphiteNameBuilder : IGraphiteNameBuilder
    {
        private const string separator = ".";
        private readonly Dictionary<string, string> namePartsByRoutingKeys = new Dictionary<string, string>();

        private readonly Dictionary<string, TagInfo> defaultTagInfos = new[]
        {
            new TagInfo(MetricsTagNames.Type, 4, null),
            new TagInfo(MetricsTagNames.Host, 3, s => s.ToLower()),
            new TagInfo(MetricsTagNames.Operation, 2, null),
            new TagInfo(MetricsTagNames.Status, 1, null)
        }.ToDictionary(x => x.Name, x => x, StringComparer.OrdinalIgnoreCase);

        public string BuildPrefix(string routingKey, IEnumerable<KeyValuePair<string, string>> tags)
        {
            var namePartByRoutingKey = GetNamePartByRoutingKey(routingKey);
            var namePartByTags = BuildNamePartByTags(tags);

            return string.IsNullOrEmpty(namePartByTags)
                ? namePartByRoutingKey
                : namePartByRoutingKey + separator + namePartByTags;
        }

        public string BuildName(string prefix, string suffix)
        {
            return prefix + separator + FixInvalidChars(suffix);
        }

        private string GetNamePartByRoutingKey(string routingKey)
        {
            if (namePartsByRoutingKeys.ContainsKey(routingKey))
            {
                return namePartsByRoutingKeys[routingKey];
            }

            var routingKeyModel = RoutingKey.Parse(routingKey);
            var partNameByRoutingKey = FixInvalidChars(routingKeyModel.Project) + separator
                                       + FixInvalidChars(routingKeyModel.Environment) + separator
                                       + FixInvalidChars(routingKeyModel.ServiceName);

            namePartsByRoutingKeys.Add(routingKey, partNameByRoutingKey);
            return partNameByRoutingKey;
        }

        private string BuildNamePartByTags(IEnumerable<KeyValuePair<string, string>> tags)
        {
            var parts = tags
                .Where(x => !string.IsNullOrEmpty(x.Key))
                .Where(x => !string.IsNullOrEmpty(x.Value))
                .Select(x => new
                {
                    x.Key,
                    x.Value,
                    TagInfo = defaultTagInfos.TryGetValue(x.Key, out var tagInfo) ? tagInfo : null
                })
                .Select(x => new
                {
                    Key = x.Key.ToLower(),
                    Value = x.TagInfo?.ConvertValue?.Invoke(x.Value) ?? x.Value,
                    Priority = x.TagInfo?.Priority ?? int.MinValue
                })
                .OrderByDescending(x => x.Priority)
                .ThenBy(x => x.Key)
                .Select(x => FixInvalidChars(x.Value));

            return string.Join(separator, parts);
        }

        private static string FixInvalidChars(string name)
        {
            if (name.All(IsPermittedSymbol))
            {
                return name;
            }

            var stringBuilder = new StringBuilder(name);
            for (var i = 0; i < stringBuilder.Length; i++)
            {
                if (!IsPermittedSymbol(stringBuilder[i]))
                {
                    stringBuilder[i] = '_';
                }
            }

            return stringBuilder.ToString();
        }

        private static bool IsPermittedSymbol(char c)
        {
            return c >= 'a' && c <= 'z'
                   || c >= 'A' && c <= 'Z'
                   || c >= '0' && c <= '9'
                   || c == '_'
                   || c == '-';
        }
    }
}
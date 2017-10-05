using System.Collections.Generic;
using Vostok.Metrics;
using Vostok.Tracing;

namespace Vostok.AirlockConsumer.TracesToEvents
{
    internal static class MetricEventBuilder
    {
        public static MetricEvent Build(Span span)
        {
            var tags = BuildTags(span);
            var values = BuildValues(span);

            return new MetricEvent
            {
                Timestamp = span.EndTimestamp.Value,
                Tags = tags,
                Values = values
            };
        }

        private static IReadOnlyDictionary<string, double> BuildValues(Span span)
        {
            return new Dictionary<string, double>
            {
                ["duration"] = (span.EndTimestamp.Value - span.BeginTimestamp).TotalMilliseconds
            };
        }

        private static Dictionary<string, string> BuildTags(Span span)
        {
            var result = new Dictionary<string, string>();

            if (span.Annotations.TryGetValue("host", out var host))
            {
                result["host"] = host;
            }

            if (span.Annotations.TryGetValue("http.code", out var httpCode))
            {
                result["status"] = httpCode;
            }

            if (span.Annotations.TryGetValue("operationName", out var operationName))
            {
                result["operation"] = operationName;
            }

            result["type"] = "requests";

            return result;
        }
    }
}
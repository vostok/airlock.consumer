using System.Collections.Generic;
using Vostok.Metrics;
using Vostok.Tracing;

namespace Vostok.AirlockConsumer.MetricsAggregator.TracesToEvents
{
    public static class MetricEventBuilder
    {
        private const string emptyTagValue = "emtpy";
        public static MetricEvent Build(Span span)
        {
            var tags = BuildTags(span);
            var values = BuildValues(span);

            return new MetricEvent
            {
                // ReSharper disable once PossibleInvalidOperationException - сюда попадают только метрики, где EndTimestamp!=null
                Timestamp = span.EndTimestamp.Value,
                Tags = tags,
                Values = values
            };
        }

        private static IReadOnlyDictionary<string, double> BuildValues(Span span)
        {
            return new Dictionary<string, double>
            {
                // ReSharper disable once PossibleInvalidOperationException - сюда попадают только метрики, где EndTimestamp!=null
                [MetricsValueNames.Duration] = (span.EndTimestamp.Value - span.BeginTimestamp).TotalMilliseconds
            };
        }

        private static Dictionary<string, string> BuildTags(Span span)
        {
            var result = new Dictionary<string, string>();

            if (span.Annotations.TryGetValue(TracingAnnotationNames.Host, out var host))
            {
                result[MetricsTagNames.Host] = host;
            }
            else
            {
                result[MetricsTagNames.Host] = emptyTagValue;
            }

            if (span.Annotations.TryGetValue(TracingAnnotationNames.HttpCode, out var httpCode))
            {
                result[MetricsTagNames.Status] = httpCode;
            }
            else
            {
                result[MetricsTagNames.Status] = emptyTagValue;
            }

            if (span.Annotations.TryGetValue(TracingAnnotationNames.Operation, out var operation))
            {
                result[MetricsTagNames.Operation] = operation;
            }
            else
            {
                result[MetricsTagNames.Operation] = emptyTagValue;
            }

            result[MetricsTagNames.Type] = "requests";

            return result;
        }
    }
}
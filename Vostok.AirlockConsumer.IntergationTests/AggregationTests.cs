using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using Vostok.Airlock;
using Vostok.Metrics;
using Vostok.Tracing;

namespace Vostok.AirlockConsumer.IntergationTests
{
    public class AggregationTests
    {
        private static readonly string tracesRoutingKey = RoutingKey.Create(IntegrationTestsEnvironment.Project, IntegrationTestsEnvironment.Environment, nameof(TracingTests), RoutingKey.TracesSuffix);
        private static readonly string appEventsRoutingKey = RoutingKey.Create(IntegrationTestsEnvironment.Project, IntegrationTestsEnvironment.Environment, nameof(TracingTests), RoutingKey.AppEventsSuffix);

        [Test]
        [Category("Load")]
        public void PushManyHttpSpansToAirlock()
        {
            PushToAirlock(GenerateSpans(count: 1000));
        }
        [Test]
        [Category("Load")]
        public void PushAppEventsToAirlock()
        {
            PushToAirlock(GenerateMetrics(count: 100));
        }

        private static Span[] GenerateSpans(int count)
        {
            var utcNow = DateTimeOffset.UtcNow;
            var random = new Random();
            return Enumerable.Range(0, count)
                .Select(i => new Span
                {
                    BeginTimestamp = utcNow.AddSeconds(-i * 10),
                    EndTimestamp = utcNow.AddSeconds((-i + 10) * 10),
                    SpanId = Guid.NewGuid(),
                    TraceId = Guid.NewGuid(),
                    Annotations = new Dictionary<string, string>
                    {
                        [TracingAnnotationNames.Kind] = "http-server",
                        [TracingAnnotationNames.Host] = "localhost",
                        [TracingAnnotationNames.HttpCode] = (200 + random.Next(0,10)).ToString(),
                        [TracingAnnotationNames.Operation] = "oper"
                    }
                }).ToArray();
        }
        private static MetricEvent[] GenerateMetrics(int count)
        {
            var utcNow = DateTimeOffset.UtcNow;
            var random = new Random();
            return Enumerable.Range(0, count)
                .Select(i => new MetricEvent
                {
                    Timestamp = utcNow.AddSeconds(-i * 10),
                    Values = new Dictionary<string, double> { ["testval"] = random.Next(20) },
                    Tags = new Dictionary<string, string> { [MetricsTagNames.Host] = "testhost" },
                }).ToArray();
        }

        private static void PushToAirlock(Span[] spans)
        {
            IntegrationTestsEnvironment.PushToAirlock(tracesRoutingKey, spans, e => e.BeginTimestamp);
        }

        private static void PushToAirlock(MetricEvent[] spans)
        {
            IntegrationTestsEnvironment.PushToAirlock(appEventsRoutingKey, spans, e => e.Timestamp);
        }

    }
}
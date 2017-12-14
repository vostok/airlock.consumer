using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using Vostok.Airlock;
using Vostok.Tracing;

namespace Vostok.AirlockConsumer.IntergationTests
{
    public class AggreationTests
    {
        private static readonly string tracesRoutingKey = RoutingKey.Create(IntegrationTestsEnvironment.Project, IntegrationTestsEnvironment.Environment, nameof(TracingTests), RoutingKey.TracesSuffix);
        private static readonly string appEventsRoutingKey = RoutingKey.Create(IntegrationTestsEnvironment.Project, IntegrationTestsEnvironment.Environment, nameof(TracingTests), RoutingKey.AppEventsSuffix);

        [Test]
        [Category("Load")]
        public void PushManyHttpSpansToAirlock()
        {
            PushToAirlock(GenerateSpans(count: 100));
        }

        private static Span[] GenerateSpans(int count)
        {
            var utcNow = DateTimeOffset.UtcNow;
            var random = new Random();
            return Enumerable.Range(0, count)
                .Select(i => new Span
                {
                    BeginTimestamp = utcNow.AddMilliseconds(-i * 10),
                    EndTimestamp = utcNow.AddMilliseconds((-i - 10) * 10),
                    SpanId = Guid.NewGuid(),
                    TraceId = Guid.NewGuid(),
                    Annotations = new Dictionary<string, string>
                    {
                        ["kind"] = "http-server",
                        [TracingAnnotationNames.Host] = "localhost",
                        [TracingAnnotationNames.HttpCode] = (200 + random.Next(0,10)).ToString(),
                        [TracingAnnotationNames.Operation] = "oper"
                    }
                }).ToArray();
        }

        private static void PushToAirlock(Span[] spans)
        {
            IntegrationTestsEnvironment.PushToAirlock(tracesRoutingKey, spans, e => e.BeginTimestamp);
        }

    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using Vstk.Airlock;
using Vstk.Tracing;

namespace Vstk.AirlockConsumer.IntergationTests
{
    public class TraceEventsTests
    {
        private static readonly string tracesRoutingKey = RoutingKey.Create(IntegrationTestsEnvironment.Project, IntegrationTestsEnvironment.Environment, nameof(TracingTests), RoutingKey.TracesSuffix);

        [Test]
        [Category("Load")]
        public void PushManyTraceEventsToAirlock()
        {
            PushToAirlock(GenerateSpans(count: 1000));
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

        private static void PushToAirlock(Span[] spans)
        {
            IntegrationTestsEnvironment.PushToAirlock(tracesRoutingKey, spans, e => e.BeginTimestamp);
        }

    }
}
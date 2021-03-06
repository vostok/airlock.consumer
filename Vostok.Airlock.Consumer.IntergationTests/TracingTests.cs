﻿using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using Vostok.Contrails.Client;
using Vostok.Logging;
using Vostok.Tracing;

namespace Vostok.Airlock.Consumer.IntergationTests
{
    public class TracingTests
    {
        private static readonly string routingKey = RoutingKey.Create(IntegrationTestsEnvironment.Project, IntegrationTestsEnvironment.Environment, nameof(TracingTests), RoutingKey.TracesSuffix);

        [Test]
        public void SendTraceEventsToAirlock_GotItAtCassandra()
        {
            const int eventCount = 10;
            var spans = GenerateSpans(eventCount);
            PushToAirlock(spans);

            var contrailsClient = new ContrailsClient(new ContrailsClientSettings
            {
                CassandraNodes = new[] {"localhost:9042"},
                Keyspace = "airlock",
                CassandraRetryExecutionStrategySettings = new CassandraRetryExecutionStrategySettings(),
            }, IntegrationTestsEnvironment.Log);

            WaitHelper.Wait(
                () =>
                {
                    foreach (var span in spans)
                    {
                        var tracesById = contrailsClient.GetTracesById(span.TraceId, null, null, null, null, true).GetAwaiter().GetResult().ToArray();
                        if (tracesById.Length == 0)
                            return false;
                        var spanResult = tracesById[0];

                        IntegrationTestsEnvironment.Log.Debug("got span " + spanResult.ToJson());
                        Assert.AreEqual(1, tracesById.Length);
                        Assert.AreEqual(span.SpanId, spanResult.SpanId);
                    }
                    return true;
                });
        }

        [Test]
        [Category("Load")]
        public void PushManySpansToAirlock()
        {
            PushToAirlock(GenerateSpans(count: 1_000_000));
        }

        private static Span[] GenerateSpans(int count)
        {
            var utcNow = DateTimeOffset.UtcNow;
            var testId = Guid.NewGuid().ToString("N");
            return Enumerable.Range(0, count)
                             .Select(i => new Span
                             {
                                 BeginTimestamp = utcNow.AddMilliseconds(-i*10),
                                 EndTimestamp = utcNow.AddMilliseconds((-i - 10)*10),
                                 SpanId = Guid.NewGuid(),
                                 TraceId = Guid.NewGuid(),
                                 Annotations = new Dictionary<string, string> {["testId"] = testId}
                             }).ToArray();
        }

        private static void PushToAirlock(Span[] spans)
        {
            IntegrationTestsEnvironment.PushToAirlock(routingKey, spans, e => e.BeginTimestamp);
        }
    }
}
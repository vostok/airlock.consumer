using System;
using System.Collections.Generic;
using NUnit.Framework;
using Vostok.AirlockConsumer.Tracing;
using Vostok.Tracing;

namespace Vostok.AirlockConsumer.Tests.Tracing
{
    public class ProcessorTests
    {
        [Test, Explicit("Manual")]
        public void ProcessData()
        {
            var processor = new TracingAirlockEventProcessor(CassandraTest.DataScheme);
            processor.ProcessAsync(
                new List<AirlockEvent<Span>>
                {
                    new AirlockEvent<Span>
                    {
                        Payload = new Span
                        {
                            Annotations = new Dictionary<string, string>
                            {
                                ["host"] = "localhost",
                                ["kind"] = "cluster-client",
                            },
                            TraceId = Guid.NewGuid(),
                            BeginTimestamp = DateTimeOffset.UtcNow,
                            EndTimestamp = DateTimeOffset.UtcNow.AddMinutes(7),
                            SpanId = Guid.NewGuid(),
                            ParentSpanId = Guid.NewGuid()
                        },
                        Timestamp = DateTimeOffset.UtcNow
                    },
                    new AirlockEvent<Span>
                    {
                        Payload = new Span
                        {
                            Annotations = new Dictionary<string, string>
                            {
                                ["host"] = "localhost",
                                ["kind"] = "http-server",
                            },
                            TraceId = Guid.NewGuid(),
                            BeginTimestamp = DateTimeOffset.UtcNow,
                            EndTimestamp = DateTimeOffset.UtcNow.AddMinutes(7),
                            SpanId = Guid.NewGuid(),
                            ParentSpanId = Guid.NewGuid()
                        },
                        Timestamp = DateTimeOffset.UtcNow
                    },
                }).GetAwaiter().GetResult();
        }
    }
}
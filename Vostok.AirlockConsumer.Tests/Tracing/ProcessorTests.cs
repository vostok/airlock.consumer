using System;
using System.Collections.Generic;
using NSubstitute;
using NUnit.Framework;
using Vostok.AirlockConsumer.Tracing;
using Vostok.Logging;
using Vostok.Tracing;

namespace Vostok.AirlockConsumer.Tests.Tracing
{
    public class ProcessorTests
    {
        [Test, Explicit("Manual")]
        public void ProcessData()
        {
            var retryExecutionStrategySettings = new CassandraRetryExecutionStrategySettings();
            var retryExecutionStrategy = new CassandraRetryExecutionStrategy(retryExecutionStrategySettings, Substitute.For<ILog>(), CassandraTest.Session.Value);

            var processor = new TracingAirlockEventProcessor(CassandraTest.DataScheme, retryExecutionStrategy);
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
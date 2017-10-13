using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NSubstitute;
using NUnit.Framework;
using Vostok.AirlockConsumer.Tracing;
using Vostok.Contrails.Client;
using Vostok.Logging;
using Vostok.Tracing;

namespace Vostok.AirlockConsumer.Tests.Tracing
{
    public class ProcessorTests
    {
        [Test]
        public void MultiThreadingProcess()
        {
            var counter = 0;
            var contrailsClient = Substitute.For<IContrailsClient>();
            contrailsClient.AddSpan(Arg.Any<Span>()).Returns(x => 
            {
                Console.WriteLine($"start process {counter} [{Thread.CurrentThread.ManagedThreadId}]");
                Thread.Sleep(500);
                Interlocked.Increment(ref counter);
                Console.WriteLine($"processed {counter} [{Thread.CurrentThread.ManagedThreadId}]");
                return Task.CompletedTask;
            });
            var processor = new TracingAirlockEventProcessor(contrailsClient, 3);
            var airlockEvents = new List<AirlockEvent<Span>>();
            const int spanCount = 10;
            for (var i = 0; i < spanCount; i++)
            {
                airlockEvents.Add(new AirlockEvent<Span>
                {
                    Payload = new Span(),
                    Timestamp = DateTimeOffset.UtcNow
                });
            }
            Console.WriteLine("Start");
            processor.Process(airlockEvents);
            Console.WriteLine("Finish");
            Assert.AreEqual(spanCount, counter);
        }

        [Test, Ignore("Manual")]
        public void ProcessData()
        {
            //var retryExecutionStrategySettings = new CassandraRetryExecutionStrategySettings();
            //var retryExecutionStrategy = new CassandraRetryExecutionStrategy(retryExecutionStrategySettings, Substitute.For<ILog>(), CassandraTest.Session.Value);

            var contrailsClientSettings = new ContrailsClientSettings()
            {
                CassandraRetryExecutionStrategySettings = new CassandraRetryExecutionStrategySettings(),
                Keyspace = "airlock",
                CassandraNodes = new [] { "localhost:9042" }
            };
            var contrailsClient = new ContrailsClient(contrailsClientSettings, Substitute.For<ILog>());
            var processor = new TracingAirlockEventProcessor(contrailsClient, 1000);
            processor.Process(
                new List<AirlockEvent<Span>>
                {
                    new AirlockEvent<Span>
                    {
                        Payload = new Span
                        {
                            Annotations = new Dictionary<string, string>
                            {
                                [TracingAnnotationNames.Host] = "localhost",
                                [TracingAnnotationNames.Kind] = "cluster-client",
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
                                [TracingAnnotationNames.Host] = "localhost",
                                [TracingAnnotationNames.Kind] = "http-server",
                            },
                            TraceId = Guid.NewGuid(),
                            BeginTimestamp = DateTimeOffset.UtcNow,
                            EndTimestamp = DateTimeOffset.UtcNow.AddMinutes(7),
                            SpanId = Guid.NewGuid(),
                            ParentSpanId = Guid.NewGuid()
                        },
                        Timestamp = DateTimeOffset.UtcNow
                    },
                });
        }
    }
}
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;
using NSubstitute;
using NUnit.Framework;
using Vostok.AirlockConsumer.Tracing;
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
            var executionStrategy = Substitute.For<ICassandraRetryExecutionStrategy>();
            executionStrategy.ExecuteAsync(Arg.Any<Statement>()).Returns(x => 
            {
                Console.WriteLine($"start process {counter} [{Thread.CurrentThread.ManagedThreadId}]");
                Thread.Sleep(500);
                Interlocked.Increment(ref counter);
                Console.WriteLine($"processed {counter} [{Thread.CurrentThread.ManagedThreadId}]");
                return Task.CompletedTask;
            });
            var processor = new TracingAirlockEventProcessor(Substitute.For<ICassandraDataScheme>(), executionStrategy, 3);
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
            processor.ProcessAsync(airlockEvents).Wait();
            Console.WriteLine("Finish");
            Assert.NotZero(counter);
            var prevCounter = counter;
            Thread.Sleep(3000);
            Console.WriteLine($"counter after pause: {counter}");
            Assert.Greater(counter, prevCounter);
            processor.Dispose();
            Console.WriteLine("Disposed");
            Assert.AreEqual(spanCount, counter);
        }

        [Test, Ignore("Manual")]
        public void ProcessData()
        {
            var retryExecutionStrategySettings = new CassandraRetryExecutionStrategySettings();
            var retryExecutionStrategy = new CassandraRetryExecutionStrategy(retryExecutionStrategySettings, Substitute.For<ILog>(), CassandraTest.Session.Value);

            var processor = new TracingAirlockEventProcessor(CassandraTest.DataScheme, retryExecutionStrategy, 1000);
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
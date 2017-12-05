using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NSubstitute;
using NUnit.Framework;
using Vostok.AirlockConsumer.Tracing;
using Vostok.Contrails.Client;
using Vostok.Logging;
using Vostok.Logging.Logs;
using Vostok.Metrics.Meters;
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
            contrailsClient.AddSpan(Arg.Any<Span>())
                .Returns(
                    x =>
                    {
                        Console.WriteLine($"start process {counter} [{Thread.CurrentThread.ManagedThreadId}]");
                        Thread.Sleep(500);
                        Interlocked.Increment(ref counter);
                        Console.WriteLine($"processed {counter} [{Thread.CurrentThread.ManagedThreadId}]");
                        return Task.CompletedTask;
                    });
            var processor = new TracingAirlockEventProcessor(contrailsClient, Substitute.For<ILog>(), maxCassandraTasks: 3);
            var airlockEvents = new List<AirlockEvent<Span>>();
            const int spanCount = 10;
            for (var i = 0; i < spanCount; i++)
            {
                airlockEvents.Add(
                    new AirlockEvent<Span>
                    {
                        Payload = new Span(),
                        Timestamp = DateTimeOffset.UtcNow
                    });
            }
            Console.WriteLine("Start");

            processor.Process(airlockEvents, Substitute.For<ICounter>());
            Console.WriteLine("Finish");
            Assert.AreEqual(spanCount, counter);
        }
    }
}
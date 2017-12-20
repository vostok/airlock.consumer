using System;
using System.Linq;
using FluentAssertions;
using NSubstitute;
using NUnit.Framework;
using SharpRaven.Data;
using Vostok.Airlock.Logging;
using Vostok.AirlockConsumer.Sentry;
using Vostok.Logging;
using Vostok.Logging.Logs;
using Vostok.Metrics.Meters;

namespace Vostok.AirlockConsumer.UnitTests.Sentry
{
    public class SentryFilteringTest
    {
        [Test]
        [TestCase(1, 10, 200, 30)]
        [TestCase(2, 20, 200, 30)]
        [TestCase(2, 20, 2, 30)]
        [TestCase(2, 20, 0, 30)]
        [TestCase(1,100,10000,1000)]
        public void FilteringTest(int throttlingPeriodMinutes, int throttlingThreshold, int eventCount, int eventsPerPeriod)
        {
            var packetSender = Substitute.For<ISentryPacketSender>();
            var throttlingPeriod = throttlingPeriodMinutes.Minutes();
            var processor = new SentryAirlockProcessor(packetSender, new SilentLog(), 100, throttlingPeriod, throttlingThreshold);
            var utcNow = DateTimeOffset.UtcNow;
            var normalizedNow = new DateTimeOffset(utcNow.Ticks - utcNow.Ticks%throttlingPeriod.Ticks, TimeSpan.Zero);
            var eventStep = throttlingPeriod/eventsPerPeriod;
            var logEvents = Enumerable.Range(0, eventCount)
                .Select(
                    i => new LogEventData
                    {
                        Message = "hello!" + i,
                        Level = LogLevel.Error,
                        Timestamp = normalizedNow.Add(i*eventStep),
                    })
                .Select(x => new AirlockEvent<LogEventData> {Payload = x, Timestamp = x.Timestamp})
                .ToList();
            processor.Process(logEvents, new TestProcessorMetrics());
            packetSender.Received(eventCount/eventsPerPeriod*throttlingThreshold + Math.Min(eventCount%eventsPerPeriod, throttlingThreshold)).SendPacket(Arg.Any<JsonPacket>(), Arg.Any<ICounter>());
        }
    }
}
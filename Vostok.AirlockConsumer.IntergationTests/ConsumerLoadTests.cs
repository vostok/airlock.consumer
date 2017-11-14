using System;
using System.Collections.Generic;
using NUnit.Framework;
using Vostok.Airlock;
using Vostok.Airlock.Metrics;
using Vostok.Metrics;
using Vostok.Tracing;

namespace Vostok.AirlockConsumer.IntergationTests
{
    [Category("Load")]
    public class ConsumerLoadTests : BaseTestClass
    {
        [Test]
        public void SendLogEvents()
        {
            var dateTimeOffset = DateTimeOffset.UtcNow;
            var testId = Guid.NewGuid().ToString("N");

            SendLogEvents(1000000, dateTimeOffset, testId);
        }

        [Test]
        public void SentTraces()
        {
            SendTraces(1000000);
        }

        [Test]
        public void SendAppEvents()
        {
            const int eventCount = 10000;
            var dateTimeOffset = DateTimeOffset.UtcNow;

            var tags = new Dictionary<string, string> { [MetricsTagNames.Type] = "test" };
            var values = new Dictionary<string, double> { [MetricsTagNames.Type] = 1 };

            Send(
                eventCount,
                new MetricEventSerializer(),
                i =>
                {
                    var span = new MetricEvent
                    {
                        Timestamp = dateTimeOffset.AddMilliseconds(-i * 10),
                        Tags = tags,
                        Values = values
                    };
                    return span;
                },
                RoutingKey.AppEventsSuffix,
                e => e.Timestamp);
        }

    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using Vostok.Airlock;
using Vostok.Airlock.Metrics;
using Vostok.Metrics;

namespace Vostok.AirlockConsumer.IntergationTests
{
    [Category("Load")]
    public class ConsumerLoadTests : BaseTestClass
    {
        protected override bool UseAirlockClient => false;

        [Test]
        public void SendLogEvents()
        {
            SendLogEvents(1000000);
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

            var tags = new Dictionary<string, string> { [MetricsTagNames.Type] = "app" };
            var values = new Dictionary<string, double> { [MetricsTagNames.Type] = 1 };

            Send(
                new MetricEventSerializer(),
                Enumerable.Range(0, eventCount).Select(
                i =>
                {
                    var span = new MetricEvent
                    {
                        Timestamp = dateTimeOffset.AddMilliseconds(-i * 10),
                        Tags = tags,
                        Values = values
                    };
                    return span;
                }),
                RoutingKey.AppEventsSuffix,
                e => e.Timestamp);
        }

    }
}
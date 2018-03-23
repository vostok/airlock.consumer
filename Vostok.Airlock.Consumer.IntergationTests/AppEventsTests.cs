using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using Vostok.Metrics;

namespace Vostok.Airlock.Consumer.IntergationTests
{
    public class AppEventsTests
    {
        private static readonly string routingKey = RoutingKey.Create(IntegrationTestsEnvironment.Project, IntegrationTestsEnvironment.Environment, nameof(AppEventsTests), RoutingKey.AppEventsSuffix);

        [Test]
        [Category("Load")]
        public void PushManyAppEventsToAirlock()
        {
            PushToAirlock(GenerateMetricEvents(count: 10_000));
        }

        private static MetricEvent[] GenerateMetricEvents(int count)
        {
            var utcNow = DateTimeOffset.UtcNow;
            var tags = new Dictionary<string, string> {[MetricsTagNames.Type] = "app"};
            var values = new Dictionary<string, double> {[MetricsTagNames.Type] = 1};
            return Enumerable.Range(0, count)
                             .Select(i =>
                             {
                                 var span1 = new MetricEvent
                                 {
                                     Timestamp = utcNow.AddMilliseconds(-i*10),
                                     Tags = tags,
                                     Values = values
                                 };
                                 return span1;
                             }).ToArray();
        }

        private static void PushToAirlock(MetricEvent[] metricEvents)
        {
            IntegrationTestsEnvironment.PushToAirlock(routingKey, metricEvents, e => e.Timestamp);
        }
    }
}
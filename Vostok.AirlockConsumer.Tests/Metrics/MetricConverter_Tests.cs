using System;
using System.Collections.Generic;
using FluentAssertions;
using NSubstitute;
using NUnit.Framework;
using Vostok.AirlockConsumer.Metrics;
using Vostok.Graphite.Client;
using Vostok.Metrics;

namespace Vostok.AirlockConsumer.Tests.Metrics
{
    public class MetricConverter_Tests
    {
        private IGraphiteNameBuilder graphiteNameBuilder;
        private MetricConverter metricConverter;

        [SetUp]
        public void SetUp()
        {
            graphiteNameBuilder = Substitute.For<IGraphiteNameBuilder>();
            metricConverter = new MetricConverter(graphiteNameBuilder);
        }

        [Test]
        public void Convert_should_build_metrics()
        {
            const string routingKey = "routingKey";
            var tags = new Dictionary<string, string>();
            var values = new Dictionary<string, double>
            {
                ["inputName1"] = 25.5,
                ["inputName2"] = 50
            };
            var metricEvent = new MetricEvent
            {
                Tags = tags,
                Values = values,
                Timestamp = new DateTimeOffset(2017, 10, 04, 13, 40, 25, TimeSpan.FromHours(5))
            };

            const long expectingTimestamp = 1507106425L;
            const string prefixName = "prefix";
            const string name1 = "Name1";
            const string name2 = "Name2";
            graphiteNameBuilder.BuildPrefix(routingKey, tags).Returns(prefixName);
            graphiteNameBuilder.BuildName(prefixName, "inputName1").Returns(name1);
            graphiteNameBuilder.BuildName(prefixName, "inputName2").Returns(name2);

            var expectingMetrics = new []
            {
                new Metric(name1, 25.5, expectingTimestamp),
                new Metric(name2, 50, expectingTimestamp),
            };

            var actual = metricConverter.Convert(routingKey, metricEvent);

            actual.ShouldBeEquivalentTo(expectingMetrics);
        }
    }
}
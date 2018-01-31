using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using FluentAssertions;
using NSubstitute;
using NUnit.Framework;
using Vostok.Airlock;
using Vostok.Airlock.Tracing;
using Vostok.AirlockConsumer.MetricsAggregator;
using Vostok.Metrics;
using Vostok.Tracing;

namespace Vostok.AirlockConsumer.UnitTests.Aggregation
{
    public class AggregationProcessorsTests
    {
        private readonly SpanAirlockSerializer spanAirlockSerializer = new SpanAirlockSerializer();
        private readonly string routingKey = RoutingKey.Create("vostok","ci","test",RoutingKey.TracesSuffix);

        [Test]
        public void HttpServerTracesAggregationTest()
        {
            var metricSender = Substitute.For<IMetricSender>();
            var pushed = new List<MetricEvent>();
            metricSender.When(x => x.SendMetric(Arg.Any<string>(), Arg.Any<MetricEvent>())).Do(x => pushed.Add(x.ArgAt<MetricEvent>(1)));

            var metricScope = Substitute.For<IMetricScope>();

            var metricsAggregatorSettings = new MetricsAggregatorSettings
            {
                MetricAggregationPastGap = 10.Milliseconds(),
                MetricResetDaemonIterationPeriod = 100.Milliseconds()
            };
            var processor = new MetricsAggregatorProcessor(metricSender, metricScope, metricsAggregatorSettings, routingKey);
            processor.GetStartTimestampOnRebalance(routingKey);
            const int eventCount = 10;
            var spans = GenerateSpans(eventCount);
            var processorMetrics = new TestProcessorMetrics();
            processor.Process(Serialize(spans), processorMetrics);
            Thread.Sleep(1000);
            processor.Release(routingKey);
            var metricEvent = pushed.FirstOrDefault(m => m.Tags[MetricsTagNames.Host]=="any" && m.Tags[MetricsTagNames.Operation] == "any" && m.Tags[MetricsTagNames.Status]=="any");
            Assert.NotNull(metricEvent);
            Console.WriteLine($"pushed ts={metricEvent.Timestamp:s}, tags={string.Join(",", metricEvent.Tags.Select(x => $"{x.Key}=>{x.Value}"))}, values={string.Join(",", metricEvent.Values.Select(x => $"{x.Key}=>{x.Value}"))}");
            Assert.AreEqual(metricEvent.Values["count"], eventCount, 1e-10);
            // ReSharper disable once PossibleInvalidOperationException
            Assert.AreEqual(metricEvent.Values["duration_sum"], spans.Sum(x => x.Payload.EndTimestamp.Value.ToUnixTimeMilliseconds() - x.Payload.BeginTimestamp.ToUnixTimeMilliseconds()), 1e-10);
        }

        private List<AirlockEvent<byte[]>> Serialize(List<AirlockEvent<Span>> spans)
        {
            return spans.Select(x =>
            {
                var airlockSink = new NonReusableByteBufferAirlockSink();
                spanAirlockSerializer.Serialize(x.Payload, airlockSink);
                return new AirlockEvent<byte[]>
                {
                    RoutingKey = x.RoutingKey,
                    Timestamp = x.Timestamp,
                    Payload = airlockSink.FilledBuffer,
                };
            }).ToList();
        }

        private List<AirlockEvent<Span>> GenerateSpans(int count)
        {
            var utcNow = DateTimeOffset.UtcNow;
            var random = new Random();
            return Enumerable.Range(0, count)
                .Select(i => new Span
                {
                    BeginTimestamp = utcNow.AddMilliseconds(-i * 10),
                    EndTimestamp = utcNow.AddMilliseconds((-i + random.Next(1, 11)) * 10),
                    SpanId = Guid.NewGuid(),
                    TraceId = Guid.NewGuid(),
                    Annotations = new Dictionary<string, string>
                    {
                        [TracingAnnotationNames.Kind] = "http-server",
                        [TracingAnnotationNames.Host] = "localhost",
                        [TracingAnnotationNames.HttpCode] = (200 + random.Next(0, 10)).ToString(),
                        [TracingAnnotationNames.Operation] = "oper"
                    }
                }).Select(x => new AirlockEvent<Span> { Payload = x, RoutingKey = routingKey, Timestamp = x.BeginTimestamp }).ToList();
        }

    }
}
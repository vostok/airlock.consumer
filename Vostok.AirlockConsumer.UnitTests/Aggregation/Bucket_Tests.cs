using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NSubstitute;
using NUnit.Framework;
using Vostok.AirlockConsumer.MetricsAggregator;
using Vostok.Metrics;

namespace Vostok.AirlockConsumer.UnitTests.Aggregation
{
    public class Bucket_Tests
    {
        private const double doubleDelta = 1e-10;
        private readonly TimeSpan period = TimeSpan.FromSeconds(5);
        private readonly Dictionary<string, double> increment = new Dictionary<string, double> {["k1"] = 12, ["k2"] = 7};
        private readonly Dictionary<string, double> increment2 = new Dictionary<string, double> {["k1"] = 3, ["k2"] = 8};
        private Bucket bucket;
        private DateTimeOffset now;
        private Borders borders;

        [SetUp]
        public void Init()
        {
            var metricScope = Substitute.For<IMetricScope>();
            now = DateTimeOffset.UtcNow;
            borders = NormalizeBorders(new Borders(now, now.AddMinutes(10)));
            now = now = borders.Past;
            bucket = new Bucket(new AggregatorMetrics(metricScope), null, period, TimeSpan.FromMilliseconds(10), borders);
        }

        [Test]
        public void Bucket_SplitToBins()
        {
            bucket.Consume(increment, now);
            bucket.Consume(increment, now.AddSeconds(1));
            bucket.Consume(increment, now.AddSeconds(2));
            bucket.Consume(increment, now.AddSeconds(10));
            var metricEvents = Enumerable.ToArray<MetricEvent>(bucket.Flush(new Borders(borders.Future, borders.Future.AddMinutes(10))).OrderBy(m => m.Timestamp));
            //Console.WriteLine(metricEvents.ToPrettyJson());
            Assert.AreEqual(2, metricEvents.Length);
            Assert.AreEqual((double) 3, metricEvents[0].Values["count"], (double) doubleDelta);
            Assert.AreEqual((double) 1, metricEvents[1].Values["count"], (double) doubleDelta);
        }

        [Test]
        public void Bucket_IgnoreOutOfInterval()
        {
            bucket.Consume(increment, borders.Past);
            bucket.Consume(increment, borders.Past.AddSeconds(-1));
            bucket.Consume(increment, borders.Past.AddSeconds(1));
            bucket.Consume(increment, borders.Future.AddSeconds(-1));
            bucket.Consume(increment, borders.Future);
            var metricEvents = Enumerable.ToArray<MetricEvent>(bucket.Flush(new Borders(borders.Future, borders.Future.AddMinutes(10))).OrderBy(m => m.Timestamp));
            Console.WriteLine(metricEvents.ToPrettyJson());
            Assert.AreEqual(2, metricEvents.Length);
            Assert.AreEqual((double) 2, metricEvents[0].Values["count"], (double) doubleDelta);
            Assert.AreEqual((double) 1, metricEvents[1].Values["count"], (double) doubleDelta);
        }

        [Test]
        public void TimeBin_MultithreadedTest()
        {
            var timeBin = new TimeBin(DateTimeOffset.UtcNow);
            var tasks = new List<Task>();
            const int taskCount = 100;
            const int comsumeCount = 10;
            for (var i = 0; i < taskCount; i++)
            {
                var task = new Task(
                    () =>
                    {
                        for (var j = 0; j < comsumeCount; j++)
                        {
                            timeBin.Consume(increment);
                            timeBin.Consume(increment2);
                        }
                    });
                task.Start();
                tasks.Add(task);
            }
            Task.WaitAll(tasks.ToArray());
            var metricEvent = timeBin.TryFlush(null, TimeSpan.FromSeconds(1));
            Assert.Null(metricEvent);
            Thread.Sleep(1500);
            metricEvent = timeBin.TryFlush(null, TimeSpan.FromSeconds(1));
            Console.WriteLine(metricEvent.ToPrettyJson());
            var metricGroups = metricEvent.Values.Keys.Where(k => k != "count").GroupBy(k => k.Split("_")[0]);
            Assert.AreEqual(increment.Count, metricGroups.Count(), "increment keys count must equal to metric groups count");
            foreach (var inc in increment)
            {
                var metricSum = metricEvent.Values[inc.Key + "_sum"];
                Assert.AreEqual(7.5, metricEvent.Values[inc.Key + "_mean"], 0.3);
                Assert.AreEqual((double) ((inc.Value + increment2[inc.Key])*taskCount*comsumeCount), metricSum, (double) doubleDelta);
            }
            Assert.AreEqual(4.5, metricEvent.Values["k1_stddev"], 0.1);
            Assert.AreEqual((double) 3, metricEvent.Values["k1_upper25"], (double) doubleDelta);
            Assert.AreEqual((double) 12, metricEvent.Values["k1_upper100"], (double) doubleDelta);

            Assert.AreEqual(0.5, metricEvent.Values["k2_stddev"], 0.1);
            Assert.AreEqual((double) 7, metricEvent.Values["k2_upper25"], (double) doubleDelta);
            Assert.AreEqual((double) 8, metricEvent.Values["k2_upper100"], (double) doubleDelta);

            metricEvent = timeBin.TryFlush(null);
            Assert.Null(metricEvent);
        }

        private Borders NormalizeBorders(Borders b)
        {
            return new Borders(NormalizeTimestamp(b.Past), NormalizeTimestamp(b.Future));
        }

        private DateTimeOffset NormalizeTimestamp(DateTimeOffset timestamp)
        {
            return new DateTimeOffset(timestamp.Ticks - timestamp.Ticks%period.Ticks, TimeSpan.Zero);
        }
    }
}
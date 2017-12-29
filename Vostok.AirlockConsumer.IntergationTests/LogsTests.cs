using System;
using System.Collections.Generic;
using System.Linq;
using Elasticsearch.Net;
using Newtonsoft.Json.Linq;
using NUnit.Framework;
using Vostok.Airlock;
using Vostok.Airlock.Logging;
using Vostok.Logging;
using Vostok.RetriableCall;

namespace Vostok.AirlockConsumer.IntergationTests
{
    public class LogsTests
    {
        private static readonly string routingKey = RoutingKey.Create(IntegrationTestsEnvironment.Project, IntegrationTestsEnvironment.Environment, nameof(LogsTests), RoutingKey.LogsSuffix);

        [Test]
        public void SendLogEventsToAirlock_GotItAtElastic()
        {
            const int eventCount = 10;
            var logEvents = GenerateLogEvens(eventCount);
            PushToAirlock(logEvents);

            var connectionPool = new StickyConnectionPool(new[] {new Uri("http://localhost:9200")});
            var elasticConfig = new ConnectionConfiguration(connectionPool);
            var elasticClient = new ElasticLowLevelClient(elasticConfig);
            var indexName = $"{IntegrationTestsEnvironment.Project}-{IntegrationTestsEnvironment.Environment}-{logEvents.First().Timestamp:yyyy.MM.dd}";

            var testId = logEvents.First().Properties["testId"];
            var expectedLogMessages = new HashSet<string>(logEvents.Select(x => x.Message));
            var callStrategy = new RetriableCallStrategy();
            WaitHelper.Wait(
                () =>
                {
                    var response = callStrategy.Call(() =>
                    {
                        var elasticsearchResponse = elasticClient.Search<string>(
                            indexName,
                            "LogEvent",
                            new
                            {
                                from = 0,
                                size = eventCount,
                                query = new
                                {
                                    match = new
                                    {
                                        testId,
                                    }
                                }
                            });
                        if (!elasticsearchResponse.Success)
                            throw new Exception("elastic error " + elasticsearchResponse.OriginalException);
                        return elasticsearchResponse;
                    }, ex => true, IntegrationTestsEnvironment.Log);
                    dynamic jObject = JObject.Parse(response.Body);
                    var hits = (JArray)jObject.hits.hits;
                    if (expectedLogMessages.Count != hits.Count)
                        return WaitAction.ContinueWaiting;
                    IntegrationTestsEnvironment.Log.Debug("elastic responce: " + response.Body);
                    foreach (dynamic hit in hits)
                    {
                        string message = hit._source.Message;
                        Assert.True(expectedLogMessages.Contains(message));
                    }
                    return WaitAction.StopWaiting;
                });
        }

        [Test]
        [Category("Load")]
        public void PushManyLogEventsToAirlock()
        {
            PushToAirlock(GenerateLogEvens(count: 100_000));
        }

        private static LogEventData[] GenerateLogEvens(int count)
        {
            var utcNow = DateTimeOffset.UtcNow;
            var testId = Guid.NewGuid().ToString("N");
            return Enumerable.Range(0, count)
                             .Select(i => new LogEventData
                             {
                                 Message = "hello!" + i,
                                 Level = LogLevel.Debug,
                                 Timestamp = utcNow.AddMilliseconds(-i*10),
                                 Properties = new Dictionary<string, string> {["testId"] = testId}
                             }).ToArray();
        }

        private static void PushToAirlock(LogEventData[] logEvents)
        {
            IntegrationTestsEnvironment.PushToAirlock(routingKey, logEvents, e => e.Timestamp);
        }
    }
}
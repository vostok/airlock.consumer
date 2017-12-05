using System;
using System.Collections.Generic;
using System.Linq;
using Vostok.Airlock;
using Vostok.Airlock.Logging;
using Vostok.Logging;
using Vostok.Logging.Logs;
using Vostok.Tracing;

namespace Vostok.AirlockConsumer.IntergationTests
{
    public abstract class BaseTestClass
    {
        protected const string project = "vostok";
        protected const string environment = "dev";
        protected readonly ConsoleLog Log = new ConsoleLog();
        protected readonly Dictionary<string, string> EnvironmentVariables;
        protected readonly string RoutingKeyPrefix;

        protected BaseTestClass()
        {
            EnvironmentVariables = EnvironmentVariablesFactory.GetEnvironmentVariables(Log);
            //EnvironmentVariables["AIRLOCK_GATE_ENDPOINTS"] = "http://vostok.dev.kontur.ru:6306/";
            //EnvironmentVariables["AIRLOCK_ELASTICSEARCH_ENDPOINTS"] = "http://devops-consul1.dev.kontur.ru:9200";
            //EnvironmentVariables["AIRLOCK_CASSANDRA_ENDPOINTS"] = "vm-ke-cass1:9042;vm-ke-cass2:9042;vm-ke-cass3:9042";
            RoutingKeyPrefix = RoutingKey.Create(project, environment, "test");
        }

        protected void Send<T>(IEnumerable<T> events, string routingKeySuffix, Func<T, DateTimeOffset> getTimestamp)
        {
            var routingKey = RoutingKey.ReplaceSuffix(RoutingKeyPrefix, routingKeySuffix);
            var airlockClient = TestAirlockClientFactory.CreateAirlockClient(Log, EnvironmentVariables);
            foreach (var @event in events)
            {
                airlockClient.Push(routingKey, @event, getTimestamp(@event));
            }
            airlockClient.FlushAsync().Wait();
            Log.Debug($"lost items: {airlockClient.LostItemsCount}, sent: {airlockClient.SentItemsCount}");
        }

        protected void SendLogEvents(int eventCount, Action<LogEventData> onCreate = null)
        {
            var dateTimeOffset = DateTimeOffset.UtcNow;
            var testId = Guid.NewGuid().ToString("N");

            Send(
                Enumerable.Range(0,eventCount).Select(
                i =>
                {
                    var logEventData = new LogEventData
                    {
                        Message = "hello!" + i,
                        Level = LogLevel.Debug,
                        Timestamp = dateTimeOffset.AddMilliseconds(-i*10),
                        Properties = new Dictionary<string, string> {["testId"] = testId}
                    };
                    onCreate?.Invoke(logEventData);
                    return logEventData;
                }),
                RoutingKey.LogsSuffix,
                e => e.Timestamp);
        }

        protected void SendTraces(int eventCount, Action<Span> onCreate = null)
        {
            var dateTimeOffset = DateTimeOffset.UtcNow;
            var testId = Guid.NewGuid().ToString("N");

            Send(
                Enumerable.Range(0, eventCount).Select(
                i =>
                {
                    var span = new Span
                    {
                        BeginTimestamp = dateTimeOffset.AddMilliseconds(-i*10),
                        EndTimestamp = dateTimeOffset.AddMilliseconds((-i - 10)*10),
                        SpanId = Guid.NewGuid(),
                        TraceId = Guid.NewGuid(),
                        Annotations = new Dictionary<string, string> {["testId"] = testId}
                    };
                    onCreate?.Invoke(span);
                    return span;
                }),
                RoutingKey.TracesSuffix,
                e => e.BeginTimestamp);
        }
    }
}
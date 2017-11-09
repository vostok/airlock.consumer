using System;
using System.Collections.Generic;
using Vostok.Airlock;
using Vostok.Airlock.Logging;
using Vostok.Airlock.Tracing;
using Vostok.Logging;
using Vostok.Logging.Logs;
using Vostok.Tracing;

namespace Vostok.AirlockConsumer.IntergationTests
{
    public class BaseTestClass
    {
        protected readonly ConsoleLog Log = new ConsoleLog();
        protected readonly Dictionary<string, string> EnvironmentVariables;
        protected readonly string RoutingKeyPrefix;

        protected BaseTestClass()
        {
            EnvironmentVariables = EnvironmentVariablesFactory.GetEnvironmentVariables(Log);
            RoutingKeyPrefix = RoutingKey.Create("vostok", "dev", "test");
        }

        private void Send<T>(int eventCount, IAirlockSerializer<T> serializer, Func<int, T> factory, string routingKeySuffix, Func<T, DateTimeOffset> getTimestamp)
        {
            var routingKey = RoutingKey.ReplaceSuffix(RoutingKeyPrefix, routingKeySuffix);
            var airlockConfig = TestAirlockClientFactory.GetAirlockConfig(Log, EnvironmentVariables);
            var messageSender = new AirlockMessageSender<T>(routingKey, new RequestSender(airlockConfig, Log), Log, serializer, getTimestamp);
            for (var i = eventCount - 1; i >= 0; i--)
            {
                var eventData = factory(i);
                //airlockClient.Push(routingKey, logEventData, logEventData.Timestamp);
                messageSender.AddEvent(eventData);
                if (i%10000 == 0)
                    messageSender.SendMessage();
            }
            messageSender.SendMessage();
        }

        protected Dictionary<string, LogEventData> SendLogEvents(int eventCount, DateTimeOffset dateTimeOffset, string testId)
        {
            var logEventDictionary = new Dictionary<string, LogEventData>();
            Send(
                eventCount,
                new LogEventDataSerializer(),
                i =>
                {
                    var logEventData = new LogEventData
                    {
                        Message = "hello!" + i,
                        Level = LogLevel.Debug,
                        Timestamp = dateTimeOffset.AddMilliseconds(-i*10),
                        Properties = new Dictionary<string, string> {["testId"] = testId}
                    };
                    logEventDictionary.Add(logEventData.Message, logEventData);
                    return logEventData;
                },
                RoutingKey.LogsSuffix,
                e => e.Timestamp);
            return logEventDictionary;
        }

        protected List<Span> SendTraces(int eventCount)
        {
            var dateTimeOffset = DateTimeOffset.UtcNow;
            var testId = Guid.NewGuid().ToString("N");

            var spansList = new List<Span>();
            Send(
                eventCount,
                new SpanAirlockSerializer(),
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
                    spansList.Add(span);
                    return span;
                },
                RoutingKey.TracesSuffix,
                e => e.BeginTimestamp);
            return spansList;
        }
    }
}
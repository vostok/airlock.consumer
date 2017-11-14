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

        protected void Send<T>(int eventCount, IAirlockSerializer<T> serializer, Func<int, T> factory, string routingKeySuffix, Func<T, DateTimeOffset> getTimestamp)
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

        protected void SendLogEvents(int eventCount, Action<LogEventData> onCreate = null)
        {
            var dateTimeOffset = DateTimeOffset.UtcNow;
            var testId = Guid.NewGuid().ToString("N");

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
                    onCreate?.Invoke(logEventData);
                    return logEventData;
                },
                RoutingKey.LogsSuffix,
                e => e.Timestamp);
        }

        protected void SendTraces(int eventCount, Action<Span> onCreate = null)
        {
            var dateTimeOffset = DateTimeOffset.UtcNow;
            var testId = Guid.NewGuid().ToString("N");

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
                    onCreate?.Invoke(span);
                    return span;
                },
                RoutingKey.TracesSuffix,
                e => e.BeginTimestamp);
        }
    }
}
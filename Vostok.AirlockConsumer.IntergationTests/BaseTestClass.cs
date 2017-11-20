using System;
using System.Collections.Generic;
using System.Linq;
using Vostok.Airlock;
using Vostok.Airlock.Logging;
using Vostok.Airlock.Tracing;
using Vostok.Logging;
using Vostok.Logging.Logs;
using Vostok.Tracing;

namespace Vostok.AirlockConsumer.IntergationTests
{
    public abstract class BaseTestClass
    {
        protected readonly ConsoleLog Log = new ConsoleLog();
        protected readonly Dictionary<string, string> EnvironmentVariables;
        protected readonly string RoutingKeyPrefix;

        protected BaseTestClass()
        {
            EnvironmentVariables = EnvironmentVariablesFactory.GetEnvironmentVariables(Log);
            RoutingKeyPrefix = RoutingKey.Create("vostok", "dev", "test");
        }

        protected abstract bool UseAirlockClient { get; }

        protected void Send<T>(IAirlockSerializer<T> serializer, IEnumerable<T> events, string routingKeySuffix, Func<T, DateTimeOffset> getTimestamp)
        {
            var routingKey = RoutingKey.ReplaceSuffix(RoutingKeyPrefix, routingKeySuffix);
            if (UseAirlockClient)
            {
                var airlockClient = TestAirlockClientFactory.CreateAirlockClient(Log, EnvironmentVariables);
                foreach (var @event in events)
                {
                    airlockClient.Push(routingKey, @event, getTimestamp(@event));
                }
            }
            else
            {
                var airlockConfig = TestAirlockClientFactory.GetAirlockConfig(Log, EnvironmentVariables);
                using (var messageSender = new AirlockMessageSender<T>(routingKey, new RequestSender(airlockConfig, Log), Log, serializer, getTimestamp))
                {
                    foreach (var @event in events)
                    {
                        messageSender.AddEvent(@event);
                    }
                }
            }
        }

        protected void SendLogEvents(int eventCount, Action<LogEventData> onCreate = null)
        {
            var dateTimeOffset = DateTimeOffset.UtcNow;
            var testId = Guid.NewGuid().ToString("N");

            Send(
                new LogEventDataSerializer(),
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
                new SpanAirlockSerializer(),
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
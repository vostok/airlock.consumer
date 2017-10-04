using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading;
using FluentAssertions;
using NUnit.Framework;
using Vostok.Airlock;
using Vostok.Commons.Binary;
using Vostok.Logging;
using Vostok.Logging.Airlock;
using Vostok.Logging.Logs;

namespace Vostok.AirlockConsumer.Tests
{
    public static class AirlockEventTypes
    {
        public const short Logging = 1;
        public const short Metrics = 2;
        public const short Tracing = 3;
    }

    public class EventRecord
    {
        public long Timestamp { get; set; }
        public byte[] Data { get; set; }
    }

    public class EventGroup
    {
        public short EventType { get; set; }
        public List<EventRecord> EventRecords { get; set; }
    }

    public class AirlockMessage
    {
        public short Version { get; } = 1;
        public List<EventGroup> EventGroups { get; set; }
    }

    public class AirlockMessageSerializer : IAirlockSerializer<AirlockMessage>
    {
        public void Serialize(AirlockMessage item, IAirlockSink sink)
        {
            var writer = sink.Writer;
            writer.Write(item.Version);
            writer.WriteCollection(item.EventGroups, WriteGroup);
        }

        private static void WriteGroup(IBinaryWriter writer, EventGroup g)
        {
            writer.Write(g.EventType);
            writer.WriteCollection(g.EventRecords, WriteRecord);
        }

        private static void WriteRecord(IBinaryWriter writer, EventRecord r)
        {
            writer.Write(r.Timestamp);
            writer.Write(r.Data);
        }
    }
    public class ConsumerTest
    {
        [Test, Ignore("Integration test")]
        public void SendAndReceive()
        {
            var logEventData = SendData();

            var messageProcessor = new LogEventMessageProcessorStub();
            using (var consumerStub = new AirlockLogEventConsumerStub(messageProcessor, new ConsoleLog()))
            {
                consumerStub.Start();
                Wait(() => messageProcessor.LastEvents != null);
                messageProcessor.LastEvents.Should().Contain(e => e.Timestamp == logEventData.Timestamp);
            }
        }

        private LogEventData SendData()
        {
            var logEventData = new LogEventData
            {
                Timestamp = DateTimeOffset.UtcNow,
                Level = LogLevel.Info,
                Message = "Hello",
                Properties = new Dictionary<string, string>
                {
                    ["host"] = "superserver"
                },
            };
            var logEventDataBytes = SerializeForAirlock<LogEventData, LogEventDataSerializer>(logEventData);
            var airlockMessage = new AirlockMessage()
            {
                EventGroups = new List<EventGroup>()
                {
                    new EventGroup()
                    {
                        EventType = AirlockEventTypes.Logging,
                        EventRecords = new List<EventRecord>()
                        {
                            new EventRecord()
                            {
                                Data = logEventDataBytes,
                                Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                            }
                        }
                    }
                }
            };
            var airlockMessageBytes = SerializeForAirlock<AirlockMessage, AirlockMessageSerializer>(airlockMessage);
            using (var httpClient = new HttpClient())
            {
                var httpContent = new ByteArrayContent(airlockMessageBytes);
                httpContent.Headers.Add("apikey", "8bb9d519-ae52-4c17-ad7a-d871dbd665fe");
                var responseMessage = httpClient.PostAsync("http://localhost:8888/send", httpContent).Result;
                Console.Out.WriteLine("resp body = " + responseMessage.Content.ReadAsStringAsync().Result);
                Console.Out.WriteLine("resp code = " + responseMessage.StatusCode);
                Assert.That(responseMessage.StatusCode,  Is.EqualTo(HttpStatusCode.OK));
            }
            return logEventData;
        }

        private static void Wait(Func<bool> check)
        {
            for (var i = 0; i < 30; i++)
            {
                if (check())
                    break;
                Thread.Sleep(1000);
            }
        }

        private static byte[] SerializeForAirlock<T, TSer>(T obj)
            where TSer : IAirlockSerializer<T>, new()
        {
            var airlockSerializer = new TSer();
            var memoryStream = new MemoryStream();
            var airlockSink = new SimpleAirlockSink(memoryStream);
            airlockSerializer.Serialize(obj, airlockSink);
            return memoryStream.GetBuffer();
        }
    }
}
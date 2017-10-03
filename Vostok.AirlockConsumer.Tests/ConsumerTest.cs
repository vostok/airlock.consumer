using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading;
using Vostok.Airlock;
using Vostok.Logging;
using Xunit;
using Xunit.Abstractions;

namespace Vostok.AirlockConsumer.Tests
{
    public class ConsumerTest
    {
        private readonly ITestOutputHelper output;

        public ConsumerTest(ITestOutputHelper output)
        {
            this.output = output;
        }

        [Fact(Skip = "Integration test")]
        public void SendAndReceive()
        {
            var logEventData = SendData();

            var messageProcessor = new LogEventMessageProcessorStub();
            using (var consumerStub = new AirlockLogEventConsumerStub(messageProcessor, TestSetup.GetLogMock(output)))
            {
                consumerStub.Start();
                Wait(() => messageProcessor.LastEvents != null);
                Assert.NotNull(messageProcessor.LastEvents);
                Assert.Contains(messageProcessor.LastEvents, e => e.Timestamp == logEventData.Timestamp);
            }
        }

        //[Fact(Skip = "Integration test")]
        private LogEventData SendData()
        {
            var logEventData = new LogEventData
            {
                Properties = new Dictionary<string, string>()
                {
                    //["timestamp"] = DateTime.UtcNow.ToString("O"),
                    ["message"] = "hello world!",
                    ["host"] = "superserver"
                },
                Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
            };
            var logEventDataBytes = SerializeForAirlock<LogEventData, LogEventDataAirlockSerializer>(logEventData);
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
                output.WriteLine("resp body = " + responseMessage.Content.ReadAsStringAsync().Result);
                output.WriteLine("resp code = " + responseMessage.StatusCode);
                Assert.Equal(HttpStatusCode.OK, responseMessage.StatusCode);
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
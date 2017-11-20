using System;
using System.Collections.Generic;
using Vostok.Airlock;
using Vostok.Commons.Binary;
using Vostok.Logging;

namespace Vostok.AirlockConsumer.IntergationTests
{
    public class AirlockMessageSender<T> : IDisposable
    {
        private readonly string routingKey;
        private readonly RequestSender requestSender;
        private readonly ILog log;
        private BinaryBufferWriter writer;
        private readonly List<T> list = new List<T>();
        private readonly IAirlockSerializer<T> serializer;
        private readonly Func<T, DateTimeOffset> getTimestamp;
        private readonly BinaryBufferWriter buf = new BinaryBufferWriter(100);
        private readonly TestAirlockSink testAirlockSink;

        public AirlockMessageSender(string routingKey, RequestSender requestSender, ILog log, IAirlockSerializer<T> serializer, Func<T, DateTimeOffset> getTimestamp)
        {
            this.routingKey = routingKey;
            this.requestSender = requestSender;
            this.log = log;
            this.serializer = serializer;
            this.getTimestamp = getTimestamp;
            testAirlockSink = new TestAirlockSink { Writer = buf };
        }

        public void AddEvent(T data)
        {
            list.Add(data);
            if (list.Count >= 10000)
                Flush();

        }

        private void Flush()
        {
            if (list.Count == 0)
                return;
            writer = new BinaryBufferWriter(list.Count*100);
            writer.Write((short)1);
            writer.Write(1);
            writer.Write(routingKey);
            writer.Write(list.Count);
            foreach (var logEventData in list)
            {
                writer.Write(getTimestamp(logEventData).ToUniversalTime().ToUnixTimeMilliseconds());
                buf.Reset(1000);
                serializer.Serialize(logEventData, testAirlockSink);
                writer.Write(buf.Buffer, 0, buf.Length);
            }
            var result = requestSender.SendAsync(new ArraySegment<byte>(writer.Buffer, 0, writer.Length)).GetAwaiter().GetResult();
            if (result!=RequestSendResult.Success)
                log.Error($"{result} on {list.Count} messages");
            list.Clear();
        }

        public void Dispose()
        {
            Flush();
        }
    }
}
using System.IO;
using Confluent.Kafka.Serialization;
using Vostok.Airlock;

namespace Vostok.AirlockConsumer
{
    internal class ConsumerDeserializer<T> : IDeserializer<T>
    {
        private readonly IAirlockDeserializer<T> airlockDeserializer;

        public ConsumerDeserializer(IAirlockDeserializer<T> airlockDeserializer)
        {
            this.airlockDeserializer = airlockDeserializer;
        }

        public T Deserialize(byte[] data)
        {
            var sink = new SimpleAirlockDeserializationSink(new MemoryStream(data));
            return airlockDeserializer.Deserialize(sink);
        }
    }
}
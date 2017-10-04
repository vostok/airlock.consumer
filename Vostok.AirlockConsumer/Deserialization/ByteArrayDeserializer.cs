using Confluent.Kafka.Serialization;

namespace Vostok.AirlockConsumer.Deserialization
{
    public class ByteArrayDeserializer : IDeserializer<byte[]>
    {
        public byte[] Deserialize(byte[] data)
        {
            return data;
        }
    }
}
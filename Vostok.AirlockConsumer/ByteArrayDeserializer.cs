using Confluent.Kafka.Serialization;

namespace Vostok.AirlockConsumer
{
    public class ByteArrayDeserializer : IDeserializer<byte[]>
    {
        public byte[] Deserialize(byte[] data)
        {
            return data;
        }
    }
}
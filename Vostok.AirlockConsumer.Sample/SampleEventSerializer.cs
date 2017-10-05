using System.IO;
using Vostok.Airlock;
using Vostok.Commons.Binary;

namespace Vostok.AirlockConsumer.Sample
{
    public class SampleEventSerializer : IAirlockSerializer<SampleEvent>, IAirlockDeserializer<SampleEvent>
    {
        private const byte FormatVersion = 1;

        public SampleEvent Deserialize(IAirlockSource source)
        {
            var reader = source.Reader;
            var version = reader.ReadByte();
            if (version != FormatVersion)
                throw new InvalidDataException("invalid format version: " + version);
            return new SampleEvent
            {
                Message = reader.ReadString(),
            };
        }

        public void Serialize(SampleEvent item, IAirlockSink sink)
        {
            var writer = sink.Writer;
            writer.Write(FormatVersion);
            writer.Write(item.Message);
        }
    }
}
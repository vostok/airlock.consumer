using System.IO;
using Vstk.Airlock;
using Vstk.Commons.Binary;

namespace Vstk.AirlockConsumer.Sample
{
    public class SampleEventSerializer : IAirlockSerializer<SampleEvent>, IAirlockDeserializer<SampleEvent>
    {
        private const byte formatVersion = 1;

        public SampleEvent Deserialize(IAirlockSource source)
        {
            var reader = source.Reader;
            var version = reader.ReadByte();
            if (version != formatVersion)
                throw new InvalidDataException("Invalid format version: " + version);
            return new SampleEvent
            {
                Message = reader.ReadString(),
            };
        }

        public void Serialize(SampleEvent item, IAirlockSink sink)
        {
            var writer = sink.Writer;
            writer.Write(formatVersion);
            writer.Write(item.Message);
        }
    }
}
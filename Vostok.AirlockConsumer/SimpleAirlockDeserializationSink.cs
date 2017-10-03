using System.IO;
using Vostok.Airlock;
using Vostok.Commons.Binary;

namespace Vostok.AirlockConsumer
{
    public class SimpleAirlockDeserializationSink : IAirlockSource
    {
        public SimpleAirlockDeserializationSink(Stream stream)
        {
            ReadStream = stream;
            Reader = new SimpleBinaryReader(stream);
        }
        public Stream ReadStream { get; }
        public IBinaryReader Reader { get; }
    }
}
using System.IO;
using Vostok.Airlock;
using Vostok.Commons.Binary;

namespace Vostok.AirlockConsumer.Deserialization
{
    public class SimpleAirlockSource : IAirlockSource
    {
        public SimpleAirlockSource(Stream stream)
        {
            ReadStream = stream;
            Reader = new SimpleBinaryReader(stream);
        }

        public Stream ReadStream { get; }
        public IBinaryReader Reader { get; }
    }
}
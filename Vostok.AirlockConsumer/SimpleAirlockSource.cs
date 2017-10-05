using System.IO;
using Vostok.Airlock;
using Vostok.Commons.Binary;

namespace Vostok.AirlockConsumer
{
    public class SimpleAirlockSource : IAirlockSource
    {
        private readonly byte[] buffer;
        private Stream readStream;

        public SimpleAirlockSource(byte[] buffer)
        {
            this.buffer = buffer;
            Reader = new BinaryBufferReader(buffer, 0);
        }

        public Stream ReadStream => readStream ?? (readStream = new MemoryStream(buffer, false));
        public IBinaryReader Reader { get; }
    }
}
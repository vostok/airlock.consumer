using System;
using System.IO;
using Vstk.Airlock;
using Vstk.Commons.Binary;

namespace Vstk.AirlockConsumer
{
    public class ByteBufferAirlockSource : IAirlockSource
    {
        public ByteBufferAirlockSource(byte[] buffer)
        {
            Reader = new BinaryBufferReader(buffer, 0);
        }

        public IBinaryReader Reader { get; }

        public Stream ReadStream => throw new NotImplementedException();
    }
}
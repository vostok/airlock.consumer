using System;
using System.IO;
using Vostok.Airlock;
using Vostok.Commons.Binary;

namespace Vostok.Airlock.Consumer
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
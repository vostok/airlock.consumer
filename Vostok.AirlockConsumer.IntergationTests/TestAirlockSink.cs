using System.IO;
using Vostok.Airlock;
using Vostok.Commons.Binary;

namespace Vostok.AirlockConsumer.IntergationTests
{
    public class TestAirlockSink : IAirlockSink
    {
        public Stream WriteStream { get; set;  }
        public IBinaryWriter Writer { get; set; }
    }
}
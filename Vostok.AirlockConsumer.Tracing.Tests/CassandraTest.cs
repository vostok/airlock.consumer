using Xunit;
using Xunit.Abstractions;

namespace Vostok.AirlockConsumer.Tracing.Tests
{
    public class CassandraTest
    {
        private readonly ITestOutputHelper output;

        public CassandraTest(ITestOutputHelper output)
        {
            this.output = output;
        }

        [Fact]
        public void Test()
        {
            output.WriteLine("lalala");
        }
    }
}
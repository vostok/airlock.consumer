using System;
using NUnit.Framework;

namespace Vostok.AirlockConsumer.IntergationTests
{
    [Category("Load")]
    public class ConsumerLoadTests : BaseTestClass
    {
        [Test]
        public void SendLogEvents()
        {
            var dateTimeOffset = DateTimeOffset.UtcNow;
            var testId = Guid.NewGuid().ToString("N");

            SendLogEvents(1000000, dateTimeOffset, testId);
        }

        [Test]
        public void SentTraces()
        {
            SendTraces(1000000);
        }
    }
}
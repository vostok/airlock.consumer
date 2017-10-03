using Vostok.Logging;
using Vostok.Logging.Airlock;

namespace Vostok.AirlockConsumer.Tests
{
    public class AirlockLogEventConsumerStub : AirlockConsumer<LogEventData>
    {
        public AirlockLogEventConsumerStub(IMessageProcessor<LogEventData> messageProcessor, ILog log) :
            base(null, new[] {"topic"}, new LogEventDataSerializer(), messageProcessor, log)
        {
        }
    }
}
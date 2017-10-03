using Vostok.Airlock;
using Vostok.Logging;
using Vostok.Logging.Airlock;

namespace Vostok.AirlockConsumer.Tests
{
    public class AirlockLogEventConsumerStub : AirlockConsumer<LogEventData>
    {
        public AirlockLogEventConsumerStub(IMessageProcessor<LogEventData> messageProcessor, ILog log) :
            base(AirlockEventTypes.Logging, 1000, new LogEventDataSerializer(), messageProcessor, log)
        {
        }
    }
}
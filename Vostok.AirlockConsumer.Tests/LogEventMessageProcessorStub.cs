using System.Collections.Generic;
using System.Linq;
using Vostok.Logging.Airlock;

namespace Vostok.AirlockConsumer.Tests
{
    internal class LogEventMessageProcessorStub : IMessageProcessor<LogEventData>
    {
        public LogEventData[] LastEvents;

        public void Process(List<AirlockEvent<LogEventData>> events)
        {
            LastEvents = events.Select(x => x.Payload).ToArray();
        }
    }
}
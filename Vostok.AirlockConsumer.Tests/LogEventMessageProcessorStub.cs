using System.Collections.Generic;
using System.Linq;
using Vostok.Logging.Airlock;

namespace Vostok.AirlockConsumer.Tests
{
    internal class LogEventMessageProcessorStub : IMessageProcessor<LogEventData>
    {
        public LogEventData[] LastEvents;

        public void Process(IEnumerable<ConsumerEvent<LogEventData>> events)
        {
            LastEvents = events.Select(x => x.Event).ToArray();
        }
    }
}
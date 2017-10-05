using System.Collections.Generic;
using System.Threading.Tasks;
using Vostok.Logging;

namespace Vostok.AirlockConsumer.Sample
{
    public class SampleDataAirlockEventProcessor : IAirlockEventProcessor<SampleEvent>
    {
        private readonly ILog log;

        public SampleDataAirlockEventProcessor(ILog log)
        {
            this.log = log;
        }

        public Task ProcessAsync(List<AirlockEvent<SampleEvent>> events)
        {
            log.Info("New events batch has arrived");
            foreach (var @event in events)
                log.Info($"{@event.RoutingKey}|{@event.Timestamp:O} - {@event.Payload.Message}");
            return Task.CompletedTask;
        }
    }
}
using Vostok.Airlock;
using Vostok.Logging;

namespace Vostok.AirlockConsumer.Logs
{
    public class AirlockLogEventConsumer : AirlockConsumer<LogEventData>
    {
        public AirlockLogEventConsumer(AirlockLogEventSettings settings)
            : base(AirlockEventTypes.Logging, settings.BatchSize, new LogEventDataAirlockDeserializer(), new LogEventMessageProcessor(settings.ElasticUriList), Program.Log.ForContext<AirlockLogEventConsumer>())
        {
        }
    }
}
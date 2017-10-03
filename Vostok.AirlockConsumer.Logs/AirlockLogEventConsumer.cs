using Vostok.Airlock;
using Vostok.Logging;
using Vostok.Logging.Airlock;

namespace Vostok.AirlockConsumer.Logs
{
    public class AirlockLogEventConsumer : AirlockConsumer<LogEventData>
    {
        public AirlockLogEventConsumer(AirlockLogEventSettings settings)
            : base(AirlockEventTypes.Logging, settings.BatchSize, new LogEventDataSerializer(), new LogEventMessageProcessor(settings.ElasticUriList), Program.Log.ForContext<AirlockLogEventConsumer>())
        {
        }
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using Vostok.Logging;
using Vostok.Logging.Airlock;

namespace Vostok.AirlockConsumer.Logs
{
    public class AirlockLogEventConsumer : AirlockConsumer<LogEventData>
    {
        public AirlockLogEventConsumer(Dictionary<string, object> settings)
            : base(settings, new[] {"vostok:staging|logs"}, new LogEventDataSerializer(), new LogEventMessageProcessor(((List<object>)settings["airlock.consumer.elastic.endpoints"]).Cast<string>().Select(x => new Uri(x)).ToArray()), AirlockConsumerLogsEntryPoint.Log.ForContext<AirlockLogEventConsumer>())
        {
        }
    }
}
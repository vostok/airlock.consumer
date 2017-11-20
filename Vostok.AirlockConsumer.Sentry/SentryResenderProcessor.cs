using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using SharpRaven;
using SharpRaven.Data;
using Vostok.Airlock.Logging;
using Vostok.Logging;
using Vostok.Metrics.Meters;

namespace Vostok.AirlockConsumer.Sentry
{
    public class SentryResenderProcessor : SimpleAirlockEventProcessorBase<LogEventData>
    {
        private readonly RavenClient ravenClient;
        private const int maxSentryTasks = 1000;

        public SentryResenderProcessor(string sentryDsn, ILog log)
        {
            ravenClient = new VostokRavenClient(sentryDsn, log);
        }

        public sealed override void Process(List<AirlockEvent<LogEventData>> events, ICounter messageProcessedCounter)
        {
            Parallel.ForEach(
                events.Select(ev => ev.Payload).Where(ev => ev.Level == LogLevel.Error || ev.Level == LogLevel.Fatal),
                new ParallelOptions {MaxDegreeOfParallelism = maxSentryTasks},
                logEvent =>
                {
                    var sentryEvent = new SentryEvent(logEvent.Message)
                    {
                        Level = logEvent.Level == LogLevel.Error ? ErrorLevel.Error : ErrorLevel.Fatal,
                        Tags = logEvent.Properties,
                    };
                    sentryEvent.Tags["timestamp"] = logEvent.Timestamp.ToString("O");
                    sentryEvent.Tags["exception"] = logEvent.Exception;
                    ravenClient.Capture(sentryEvent);
                    messageProcessedCounter.Add();
                });
        }

    }
}
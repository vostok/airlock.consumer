using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using SharpRaven.Data;
using Vostok.Airlock.Logging;
using Vostok.Logging;
using Vostok.Metrics.Meters;

namespace Vostok.AirlockConsumer.Sentry
{
    public class SentryAirlockProcessor : SimpleAirlockEventProcessorBase<LogEventData>
    {
        private readonly ILog log;
        private readonly ISentryPacketSender packetSender;
        private readonly int maxSentryTasks;
        private readonly ExceptionParser exceptionParser = new ExceptionParser();
        private readonly long throttlingPeriodTicks;
        private readonly int throttlingThreshold;

        public SentryAirlockProcessor(ISentryPacketSender sentryPacketSender, ILog log, int maxSentryTasks, TimeSpan throttlingPeriod, int throttlingThreshold)
        {
            this.log = log;
            this.maxSentryTasks = maxSentryTasks;
            this.throttlingThreshold = throttlingThreshold;
            throttlingPeriodTicks = throttlingPeriod.Ticks;
            packetSender = sentryPacketSender;
        }

        public sealed override void Process(List<AirlockEvent<LogEventData>> events, ProcessorMetrics processorMetrics)
        {
            Parallel.ForEach(
                FilterEvents(events, processorMetrics.MessageIgnoredCounter),
                new ParallelOptions {MaxDegreeOfParallelism = maxSentryTasks},
                @event =>
                {
                    try
                    {
                        var logEvent = @event.Payload;
                        var jsonPacket = new JsonPacket("default")
                        {
                            Level = logEvent.Level == LogLevel.Error ? ErrorLevel.Error : ErrorLevel.Fatal,
                            Tags = logEvent.Properties,
                            TimeStamp = logEvent.Timestamp.UtcDateTime,
                            Exceptions = exceptionParser.Parse(logEvent.Exception),
                            Message = logEvent.Message,
                            MessageObject = logEvent.Message
                        };
                        JsonPacketPatcher.PatchPacket(jsonPacket);
                        packetSender.SendPacket(jsonPacket, processorMetrics.SendingErrorCounter);
                        processorMetrics.MessageProcessedCounter.Add();
                    }
                    catch (Exception e)
                    {
                        processorMetrics.MessageFailedCounter.Add();
                        log.Error(e);
                    }
                });
        }

        private IEnumerable<AirlockEvent<LogEventData>> FilterEvents(IEnumerable<AirlockEvent<LogEventData>> events, ICounter messageIgnoredCounter)
        {
            var lastTimestampIndex = 0L;
            var periodCounter = 0;
            foreach (var airlockEvent in events.Where(ev => ev.Payload.Level == LogLevel.Error || ev.Payload.Level == LogLevel.Fatal).OrderBy(ev => ev.Timestamp))
            {
                var normalizedTimestampIndex = airlockEvent.Timestamp.Ticks / throttlingPeriodTicks;
                if (normalizedTimestampIndex == lastTimestampIndex)
                {
                    periodCounter++;
                }
                else
                {
                    periodCounter = 0;
                    lastTimestampIndex = normalizedTimestampIndex;
                }
                if (periodCounter < throttlingThreshold)
                    yield return airlockEvent;
                else
                    messageIgnoredCounter.Add();
            }
        }
    }
}
using System;
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
    public class SentryAirlockProcessor : SimpleAirlockEventProcessorBase<LogEventData>
    {
        private readonly ILog log;
        private readonly SentryPacketSender packetSender;
        private readonly int maxSentryTasks;
        private readonly ExceptionParser exceptionParser = new ExceptionParser();
        private readonly string projectId;

        public SentryAirlockProcessor(IRavenClient ravenClient, ILog log, int maxSentryTasks)
        {
            this.log = log;
            packetSender = new SentryPacketSender(ravenClient.CurrentDsn, log);
            projectId = ravenClient.CurrentDsn.ProjectID;
            this.maxSentryTasks = maxSentryTasks;
        }

        public sealed override void Process(List<AirlockEvent<LogEventData>> events, ICounter messageProcessedCounter)
        {
            Parallel.ForEach(
                events.Where(ev => ev.Payload.Level == LogLevel.Error || ev.Payload.Level == LogLevel.Fatal),
                new ParallelOptions {MaxDegreeOfParallelism = maxSentryTasks},
                @event =>
                {
                    try
                    {
                        var logEvent = @event.Payload;
                        var jsonPacket = new JsonPacket(projectId)
                        {
                            Level = logEvent.Level == LogLevel.Error ? ErrorLevel.Error : ErrorLevel.Fatal,
                            Tags = logEvent.Properties,
                            TimeStamp = logEvent.Timestamp.UtcDateTime,
                            Exceptions = exceptionParser.Parse(logEvent.Exception),
                            Message = logEvent.Message,
                            MessageObject = logEvent.Message
                        };
                        JsonPacketPatcher.PatchPacket(jsonPacket);
                        log.Debug("prepared packet: " + jsonPacket.ToPrettyJson());
                        packetSender.SendPacket(jsonPacket);
                        messageProcessedCounter.Add();
                    }
                    catch (Exception e)
                    {
                        log.Error(e);
                    }
                });
        }
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using SharpRaven;
using SharpRaven.Data;
using Vostok.Airlock;
using Vostok.Airlock.Logging;
using Vostok.Logging;
using Vostok.Metrics.Meters;

namespace Vostok.AirlockConsumer.Sentry
{
    public class SentryResenderProcessor : SimpleAirlockEventProcessorBase<LogEventData>
    {
        private const int maxSentryTasks = 100;
        private readonly ILog log;
        private readonly SentryPacketSender packetSender;
        private readonly Dsn dsn;
        private readonly ExceptionParser exceptionParser = new ExceptionParser();

        public SentryResenderProcessor(string sentryDsn, ILog log)
        {
            this.log = log;
            dsn = new Dsn(sentryDsn);
            packetSender = new SentryPacketSender(dsn, log);
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
                        RoutingKey.Parse(@event.RoutingKey, out var _, out var environment, out var _, out var _);
                        var logEvent = @event.Payload;
                        var jsonPacket = new JsonPacket(dsn.ProjectID)
                        {
                            Level = logEvent.Level == LogLevel.Error ? ErrorLevel.Error : ErrorLevel.Fatal,
                            Tags = logEvent.Properties,
                            TimeStamp = logEvent.Timestamp.UtcDateTime,
                            Environment = environment,
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
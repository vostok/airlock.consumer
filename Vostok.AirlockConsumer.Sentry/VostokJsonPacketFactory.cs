using System;
using System.Globalization;
using SharpRaven.Data;
using Vostok.Logging;

namespace Vostok.AirlockConsumer.Sentry
{
    internal class VostokJsonPacketFactory : JsonPacketFactory
    {
        private readonly ILog log;
        private readonly ExceptionParser exceptionParser = new ExceptionParser();

        public VostokJsonPacketFactory(ILog log)
        {
            this.log = log;
        }

        protected override JsonPacket OnCreate(JsonPacket jsonPacket)
        {
            jsonPacket.ServerName = null;
            jsonPacket.User = new SentryUser("");
            jsonPacket.Modules.Clear();
            if (jsonPacket.Tags.TryGetValue("exception", out var exceptionStr))
            {
                if (!string.IsNullOrEmpty(exceptionStr))
                {
                    jsonPacket.Exceptions = exceptionParser.Parse(exceptionStr);
                }
                jsonPacket.Tags.Remove("exception");
            }
            if (jsonPacket.Tags.TryGetValue("timestamp", out var timestampStr))
            {
                if (!string.IsNullOrEmpty(timestampStr))
                    jsonPacket.TimeStamp = DateTime.ParseExact(timestampStr, "O", CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal);
                jsonPacket.Tags.Remove("timestamp");
            }
            log.Debug("prepared packet: " + jsonPacket.ToPrettyJson());
            if (string.IsNullOrEmpty(jsonPacket.Message))
            {
                if (jsonPacket.Exceptions != null && jsonPacket.Exceptions.Count > 0)
                {
                    jsonPacket.Message = jsonPacket.Exceptions[0].Value;
                }
                else
                {
                    jsonPacket.Message = "error";
                }
                jsonPacket.MessageObject = new SentryMessage(jsonPacket.Message);
            }
            return jsonPacket;
        }

    }
}
using System;
using System.Globalization;
using SharpRaven.Data;

namespace Vostok.AirlockConsumer.Sentry
{
    internal class VostokJsonPacketFactory : JsonPacketFactory
    {
        private readonly ExceptionParser exceptionParser = new ExceptionParser();

        protected override JsonPacket OnCreate(JsonPacket jsonPacket)
        {
            jsonPacket.ServerName = null;
            jsonPacket.User = new SentryUser("");
            jsonPacket.Modules.Clear();
            if (jsonPacket.Tags.TryGetValue("exception", out var exceptionStr))
            {
                jsonPacket.Exceptions = exceptionParser.Parse(exceptionStr);
                jsonPacket.Tags.Remove("exception");
            }
            if (jsonPacket.Tags.TryGetValue("timestamp", out var timestampStr))
            {
                if (!string.IsNullOrEmpty(exceptionStr))
                    jsonPacket.TimeStamp = DateTime.ParseExact(timestampStr, "O", CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal);
                jsonPacket.Tags.Remove("timestamp");
            }
            return jsonPacket;
        }

    }
}
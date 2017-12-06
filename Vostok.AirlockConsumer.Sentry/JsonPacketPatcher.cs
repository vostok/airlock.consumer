using SharpRaven.Data;

namespace Vostok.AirlockConsumer.Sentry
{
    public static class JsonPacketPatcher
    {
        public static void PatchPacket(JsonPacket jsonPacket)
        {
            jsonPacket.ServerName = null;
            jsonPacket.User = new SentryUser("");
            jsonPacket.Modules.Clear();
            if (jsonPacket.Tags.TryGetValue("SourceContext", out var sourceContext))
            {
                jsonPacket.Logger = sourceContext;
                jsonPacket.Tags.Remove("SourceContext");
            }
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
        }
    }
}
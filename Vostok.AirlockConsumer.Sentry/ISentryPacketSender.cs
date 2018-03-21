using SharpRaven.Data;
using Vostok.Metrics.Meters;

namespace Vostok.AirlockConsumer.Sentry
{
    public interface ISentryPacketSender
    {
        void SendPacket(JsonPacket packet, ICounter sendingErrorCounter);
    }
}
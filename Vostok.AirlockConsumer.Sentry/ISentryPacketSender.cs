using SharpRaven.Data;
using Vstk.Metrics.Meters;

namespace Vstk.AirlockConsumer.Sentry
{
    public interface ISentryPacketSender
    {
        void SendPacket(JsonPacket packet, ICounter sendingErrorCounter);
    }
}
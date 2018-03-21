using SharpRaven.Data;
using Vostok.Metrics.Meters;

namespace Vostok.Airlock.Consumer.Sentry
{
    public interface ISentryPacketSender
    {
        void SendPacket(JsonPacket packet, ICounter sendingErrorCounter);
    }
}
using System;
using System.Linq;
using System.Net;
using SharpRaven;
using SharpRaven.Data;
using Vstk.Logging;
using Vstk.Metrics.Meters;
using Vstk.RetriableCall;

namespace Vstk.AirlockConsumer.Sentry
{
    public class SentryPacketSender : ISentryPacketSender
    {
        private static readonly WebExceptionStatus[] retriableHttpStatusCodes =
        {
            WebExceptionStatus.ConnectFailure,
            WebExceptionStatus.ReceiveFailure,
            WebExceptionStatus.SendFailure,
            WebExceptionStatus.Timeout,
            WebExceptionStatus.ConnectionClosed,
            WebExceptionStatus.RequestCanceled,
            WebExceptionStatus.KeepAliveFailure,
            WebExceptionStatus.UnknownError,
        };

        private readonly ILog log;
        private readonly RavenClient ravenClient;
        private readonly RetriableCallStrategy retriableCallStrategy = new RetriableCallStrategy();

        public SentryPacketSender(RavenClient ravenClient, ILog log)
        {
            this.ravenClient = ravenClient;
            this.log = log;
        }

        public void SendPacket(JsonPacket packet, ICounter sendingErrorCounter)
        {
            retriableCallStrategy.Call(() =>
            {
                var requester = new Requester(packet, ravenClient);
                return requester.Request();
            }, e =>
            {
                sendingErrorCounter.Add();
                return IsRetriableException(e);
            }, log);
        }

        private static bool IsRetriableException(Exception e)
        {
            var webException = e.FindFirstException<WebException>();
            var httpStatusCode = webException?.Status;
            return httpStatusCode.HasValue && retriableHttpStatusCodes.Contains(httpStatusCode.Value);
        }
    }
}
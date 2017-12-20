using System;
using System.Linq;
using System.Net;
using SharpRaven;
using SharpRaven.Data;
using Vostok.Logging;
using Vostok.Metrics.Meters;
using Vostok.RetriableCall;

namespace Vostok.AirlockConsumer.Sentry
{
    public interface ISentryPacketSender
    {
        void SendPacket(JsonPacket packet, ICounter sendingErrorCounter);
    }

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
        private readonly string sentryProjectId;

        public SentryPacketSender(RavenClient ravenClient, ILog log)
        {
            this.ravenClient = ravenClient;
            this.log = log;
            sentryProjectId = ravenClient.CurrentDsn.ProjectID;
        }

        public void SendPacket(JsonPacket packet, ICounter sendingErrorCounter)
        {
            packet.Project = sentryProjectId;
            retriableCallStrategy.Call(() =>
            {
                var requester = new Requester(packet, ravenClient);
                return requester.Request();
            }, ex =>
            {
                sendingErrorCounter.Add();
                return IsRetriableException(ex);
            }, log);
        }

        private static bool IsRetriableException(Exception ex)
        {
            var webException = ExceptionFinder.FindException<WebException>(ex);
            var httpStatusCode = webException?.Status;
            if (httpStatusCode == null)
                return false;
            var statusCode = httpStatusCode.Value;
            return retriableHttpStatusCodes.Contains(statusCode);
        }
    }
}
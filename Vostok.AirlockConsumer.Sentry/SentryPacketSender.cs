using System;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using SharpRaven;
using SharpRaven.Data;
using Vostok.Logging;
using Vostok.RetriableCall;

namespace Vostok.AirlockConsumer.Sentry
{
    public class SentryPacketSender 
    {
        private readonly ILog log;
        private readonly RetriableCallStrategy retriableCallStrategy = new RetriableCallStrategy();
        private readonly RavenClient ravenClient;

        public SentryPacketSender(Dsn dsn, ILog log) 
        {
            ravenClient = new RavenClient(dsn);
            this.log = log;
        }

        public void SendPacket(JsonPacket packet)
        {
            retriableCallStrategy.Call(() =>
            {
                var requester = new Requester(packet, ravenClient);
                return requester.Request();
            }, IsRetriableException, log);
        }

        private static readonly WebExceptionStatus[] retriableHttpStatusCodes =
        {
            WebExceptionStatus.ConnectFailure,
            WebExceptionStatus.ReceiveFailure,
            WebExceptionStatus.SendFailure,
            WebExceptionStatus.Timeout,
            WebExceptionStatus.ConnectionClosed,
            WebExceptionStatus.RequestCanceled,
            WebExceptionStatus.KeepAliveFailure
        };

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
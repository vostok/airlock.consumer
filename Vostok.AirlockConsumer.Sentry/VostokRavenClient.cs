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
    public class VostokRavenClient : RavenClient
    {
        private readonly ILog log;
        private readonly RetriableCallStrategy retriableCallStrategy = new RetriableCallStrategy();
        public VostokRavenClient(string dsn, ILog log) : base(dsn, new VostokJsonPacketFactory())
        {
            this.log = log;
            ErrorOnCapture = OnSendError;
        }

        protected override string Send(JsonPacket packet)
        {
            return retriableCallStrategy.Call(() => base.Send(packet), IsRetriableException, log);
        }

        protected override Task<string> SendAsync(JsonPacket packet)
        {
            return retriableCallStrategy.CallAsync(async () => await base.SendAsync(packet), IsRetriableException, log);
        }

        private void OnSendError(Exception ex)
        {
            throw ex;
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
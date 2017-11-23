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
        public VostokRavenClient(string dsn, ILog log) : base(dsn, new VostokJsonPacketFactory(log))
        {
            this.log = log;
        }

        protected override string Send(JsonPacket packet)
        {
            return retriableCallStrategy.Call(() =>
            {
                var requester = new Requester(packet, this);
                BeforeSend?.Invoke(requester);
                return requester.Request();
            }, IsRetriableException, log);
        }

        protected override async Task<string> SendAsync(JsonPacket packet)
        {
            return await retriableCallStrategy.CallAsync(async () =>
            {
                var requester = new Requester(packet, this);
                BeforeSend?.Invoke(requester);
                return await requester.RequestAsync();
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
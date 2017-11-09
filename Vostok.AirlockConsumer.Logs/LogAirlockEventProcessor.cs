using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using Elasticsearch.Net;
using Vostok.Airlock.Logging;
using Vostok.Logging;
using Vostok.RetriableCall;

namespace Vostok.AirlockConsumer.Logs
{
    public class LogAirlockEventProcessor : SimpleAirlockEventProcessorBase<LogEventData>
    {
        private readonly ILog log;
        private readonly ElasticLowLevelClient elasticClient;
        private readonly RetriableCallStrategy retriableCallStrategy;

        public LogAirlockEventProcessor(Uri[] elasticUris, ILog log)
        {
            retriableCallStrategy = new RetriableCallStrategy();
            this.log = log;
            var connectionPool = new StickyConnectionPool(elasticUris);
            var elasticConfig = new ConnectionConfiguration(connectionPool);
            elasticClient = new ElasticLowLevelClient(elasticConfig);
        }

        public sealed override void Process(List<AirlockEvent<LogEventData>> events)
        {
            var bulkItems = new List<object>();
            foreach (var @event in events)
            {
                bulkItems.Add(BuildIndexRecordMeta(@event));
                bulkItems.Add(BuildIndexRecord(@event));
            }
            Index(bulkItems);
        }

        private void Index(List<object> bulkItems)
        {
            var postData = new PostData<object>(bulkItems);
            retriableCallStrategy.Call(() =>
            {
                var response = elasticClient.Bulk<byte[]>(postData);
                if (!response.Success)
                    throw response.OriginalException;
            }, IsRetriableException, log);
        }

        private static readonly HttpStatusCode[] retriableHttpStatusCodes =
        {
            HttpStatusCode.InternalServerError,
            HttpStatusCode.NotImplemented,
            HttpStatusCode.BadGateway,
            HttpStatusCode.GatewayTimeout,
            HttpStatusCode.RequestTimeout,
            HttpStatusCode.ServiceUnavailable,
            HttpStatusCode.TemporaryRedirect,
        };

        private bool IsRetriableException(Exception ex)
        {
            var elasticsearchClientException = ExceptionFinder.FindException<ElasticsearchClientException>(ex);
            if (elasticsearchClientException == null)
                return false;
            var statusCode = (HttpStatusCode) (elasticsearchClientException.Response.HttpStatusCode ?? 500);
            return retriableHttpStatusCodes.Contains(statusCode);
        }

        private static object BuildIndexRecordMeta(AirlockEvent<LogEventData> @event)
        {
            var indexName = string.Format("{0}-{1:yyyy.MM.dd}", @event.RoutingKey.Replace('.', '-'), @event.Payload.Timestamp.Date);
            return new
            {
                index = new
                {
                    _index = indexName,
                    _type = "LogEvent",
                }
            };
        }

        private static Dictionary<string, string> BuildIndexRecord(AirlockEvent<LogEventData> @event)
        {
            var indexRecord = new Dictionary<string, string>(StringComparer.InvariantCultureIgnoreCase)
            {
                ["Level"] = @event.Payload.Level.ToString(),
                ["@timestamp"] = @event.Payload.Timestamp.ToString("O"),
            };
            if (!string.IsNullOrEmpty(@event.Payload.Message))
                indexRecord.Add("Message", @event.Payload.Message);
            if (!string.IsNullOrEmpty(@event.Payload.Exception))
                indexRecord.Add("Exception", @event.Payload.Exception);
            foreach (var kvp in @event.Payload.Properties)
            {
                if (!indexRecord.ContainsKey(kvp.Key))
                    indexRecord.Add(kvp.Key, kvp.Value);
            }
            return indexRecord;
        }
    }
}
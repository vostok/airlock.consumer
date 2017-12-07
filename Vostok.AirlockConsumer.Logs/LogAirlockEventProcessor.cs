using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using Elasticsearch.Net;
using Vostok.Airlock;
using Vostok.Airlock.Logging;
using Vostok.Logging;
using Vostok.Metrics.Meters;
using Vostok.RetriableCall;

namespace Vostok.AirlockConsumer.Logs
{
    public class LogAirlockEventProcessor : SimpleAirlockEventProcessorBase<LogEventData>
    {
        private static readonly HttpStatusCode[] retriableHttpStatusCodes =
        {
            HttpStatusCode.BadGateway,
            HttpStatusCode.GatewayTimeout,
            HttpStatusCode.RequestTimeout,
            HttpStatusCode.ServiceUnavailable,
            HttpStatusCode.TemporaryRedirect,
        };
        private readonly ILog log;
        private readonly ElasticLowLevelClient elasticClient;
        private readonly RetriableCallStrategy retriableCallStrategy;

        public LogAirlockEventProcessor(Uri[] elasticUris, ILog log)
        {
            this.log = log;
            retriableCallStrategy = new RetriableCallStrategy();
            var connectionPool = new StickyConnectionPool(elasticUris);
            var elasticConfig = new ConnectionConfiguration(connectionPool);
            elasticClient = new ElasticLowLevelClient(elasticConfig);
        }

        public sealed override void Process(List<AirlockEvent<LogEventData>> events, ICounter messageProcessedCounter)
        {
            var bulkItems = new List<object>();
            foreach (var @event in events)
            {
                RoutingKey.Parse(@event.RoutingKey, out var project, out var environment, out var service, out var _);
                bulkItems.Add(BuildIndexRecordMeta(@event, project, environment));
                bulkItems.Add(BuildIndexRecord(@event, service));
            }
            Index(bulkItems);
            messageProcessedCounter.Add(events.Count);
        }

        private void Index(List<object> bulkItems)
        {
            var postData = new PostData<object>(bulkItems);
            retriableCallStrategy.Call(
                () =>
                {
                    var response = elasticClient.Bulk<byte[]>(postData);
                    if (!response.Success)
                        throw response.OriginalException;
                },
                IsRetriableException,
                log);
        }

        private bool IsRetriableException(Exception ex)
        {
            var elasticsearchClientException = ExceptionFinder.FindException<ElasticsearchClientException>(ex);
            var httpStatusCode = elasticsearchClientException?.Response?.HttpStatusCode;
            if (httpStatusCode == null)
                return false;
            var statusCode = (HttpStatusCode)httpStatusCode.Value;
            return retriableHttpStatusCodes.Contains(statusCode);
        }

        private static object BuildIndexRecordMeta(AirlockEvent<LogEventData> @event, string project, string environment)
        {
            var indexName = $"{project}-{environment}-{@event.Payload.Timestamp.Date:yyyy.MM.dd}";
            return new
            {
                index = new
                {
                    _index = indexName,
                    _type = "LogEvent",
                }
            };
        }

        private static Dictionary<string, string> BuildIndexRecord(AirlockEvent<LogEventData> @event, string service)
        {
            var indexRecord = new Dictionary<string, string>(StringComparer.InvariantCultureIgnoreCase)
            {
                ["Level"] = @event.Payload.Level.ToString(),
                ["@timestamp"] = @event.Payload.Timestamp.ToString("O"),
                ["@service"] = service,
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
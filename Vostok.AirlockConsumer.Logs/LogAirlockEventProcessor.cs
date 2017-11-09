using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Elasticsearch.Net;
using MoreLinq;
using Vostok.Airlock.Logging;
using Vostok.Metrics.Meters;
using Vostok.RetriableCall;

namespace Vostok.AirlockConsumer.Logs
{
    public class LogAirlockEventProcessor : SimpleAirlockEventProcessorBase<LogEventData>
    {
        private readonly ElasticLowLevelClient elasticClient;
        private readonly RetriableCallStrategy retriableCallStrategy;

        public LogAirlockEventProcessor(Uri[] elasticUris)
        {
            retriableCallStrategy = new RetriableCallStrategy();
            var connectionPool = new StickyConnectionPool(elasticUris);
            var elasticConfig = new ConnectionConfiguration(connectionPool);
            elasticClient = new ElasticLowLevelClient(elasticConfig);
        }

        public sealed override void Process(List<AirlockEvent<LogEventData>> events, ICounter messageProcessedCounter)
        {
            Parallel.ForEach(
                events.Batch(batchSize), new ParallelOptions { MaxDegreeOfParallelism = maxElasticTasks },
                batch =>
                {
                    var bulkItems = new List<object>();
                    foreach (var @event in batch)
                    {
                        bulkItems.Add(BuildIndexRecordMeta(@event));
                        bulkItems.Add(BuildIndexRecord(@event));
                    }
                    Index(bulkItems);
                    messageProcessedCounter.Add(events.Count);
                });
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

        private static readonly HttpStatusCode[] retriableHttpStatusCodes =
        {
            HttpStatusCode.BadGateway,
            HttpStatusCode.GatewayTimeout,
            HttpStatusCode.RequestTimeout,
            HttpStatusCode.ServiceUnavailable,
            HttpStatusCode.TemporaryRedirect,
        };

        private bool IsRetriableException(Exception ex)
        {
            var elasticsearchClientException = ExceptionFinder.FindException<ElasticsearchClientException>(ex);
            var httpStatusCode = elasticsearchClientException?.Response?.HttpStatusCode;
            if (httpStatusCode == null)
                return false;
            var statusCode = (HttpStatusCode) httpStatusCode.Value;
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
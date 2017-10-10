using System;
using System.Collections.Generic;
using System.Net;
using Elasticsearch.Net;
using Vostok.Logging;
using Vostok.Logging.Airlock;

namespace Vostok.AirlockConsumer.Logs
{
    // todo (avk, 04.10.2017): add metrics
    public class LogAirlockEventProcessor : SimpleAirlockEventProcessorBase<LogEventData>
    {
        private readonly ILog log;
        private readonly ElasticLowLevelClient elasticClient;

        public LogAirlockEventProcessor(Uri[] elasticUris, ILog log)
        {
            this.log = log.ForContext(this);
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

        // todo (avk, 04.10.2017): implement retry policy
        private void Index(List<object> bulkItems)
        {
            var response = elasticClient.Bulk<byte[]>(new PostData<object>(bulkItems));
            if (response.HttpStatusCode != (int)HttpStatusCode.OK)
                log.Error($"Elasic error. code= {response.HttpStatusCode}, reason: {response.ServerError?.Error?.Reason}");
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
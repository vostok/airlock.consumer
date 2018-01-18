using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Elasticsearch.Net;
using MoreLinq;
using Vostok.Airlock;
using Vostok.Airlock.Logging;
using Vostok.Logging;
using Vostok.Metrics.Meters;
using Vostok.RetriableCall;

namespace Vostok.AirlockConsumer.Logs
{
    public class LogAirlockEventProcessor : SimpleAirlockEventProcessorBase<LogEventData>
    {
        private readonly ILog log;
        private readonly ElasticLowLevelClient elasticClient;
        private readonly RetriableCallStrategy retriableCallStrategy;
        private const int maxExceptionLength = 32 * 1024;

        public LogAirlockEventProcessor(Uri[] elasticUris, ILog log)
        {
            this.log = log;
            retriableCallStrategy = new RetriableCallStrategy();
            var connectionPool = new StickyConnectionPool(elasticUris);
            var elasticConfig = new ConnectionConfiguration(connectionPool);
            elasticClient = new ElasticLowLevelClient(elasticConfig);
        }

        public sealed override void Process(List<AirlockEvent<LogEventData>> events, ProcessorMetrics processorMetrics)
        {
            var bulks = events
                .Select(
                    @event =>
                    {
                        RoutingKey.Parse(@event.RoutingKey, out var project, out var environment, out var service,
                            out var _);
                        var indexName = $"{project}-{environment}-{@event.Payload.Timestamp.Date:yyyy.MM.dd}";
                        var indexRecordMeta = BuildIndexRecordMeta(indexName);
                        var indexRecord = BuildIndexRecord(@event, service);
                        return new {indexName, indexRecordMeta, indexRecord};
                    })
                .GroupBy(x => x.indexName)
                .SelectMany(g => g.Batch(10000).Select(records =>
                {
                    var postDataItems = new List<object>();
                    foreach (var record in records)
                    {
                        postDataItems.Add(record.indexRecordMeta);
                        postDataItems.Add(record.indexRecord);
                    }

                    var postData = new PostData<object>(postDataItems);
                    return new {postData, recordsCount = postDataItems.Count / 2};
                }))
                .ToList();
            Parallel.ForEach(
                bulks,
                bulk =>
                {
                    try
                    {
                        BulkIndex(bulk.postData, processorMetrics.SendingErrorCounter);
                        processorMetrics.EventProcessedCounter.Add(bulk.recordsCount);
                    }
                    catch (Exception)
                    {
                        processorMetrics.EventFailedCounter.Add(bulk.recordsCount);
                        throw;
                    }
                });
        }

        private void BulkIndex(PostData<object> bulkIndexPostData, ICounter sendingErrorCounter)
        {
            retriableCallStrategy.Call(
                () =>
                {
                    var response = elasticClient.Bulk<byte[]>(bulkIndexPostData);
                    if (!response.Success)
                        throw new ElasticOperationFailedException(response);
                },
                ex =>
                {
                    sendingErrorCounter.Add();
                    return (ex as ElasticOperationFailedException)?.IsRetriable == true;
                },
                log);
        }

        private static object BuildIndexRecordMeta(string indexName)
        {
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
            if (@event.Payload.Exceptions != null && @event.Payload.Exceptions.Count > 0)
            {
                var exception = string.Join("\n   ---\n", @event.Payload.Exceptions).Truncate(maxExceptionLength);
                indexRecord.Add("Exception", exception);
            }

            foreach (var kvp in @event.Payload.Properties)
            {
                if (!indexRecord.ContainsKey(kvp.Key))
                    indexRecord.Add(kvp.Key, kvp.Value);
            }

            return indexRecord;
        }

        private class ElasticOperationFailedException : Exception
        {
            public ElasticOperationFailedException(ElasticsearchResponse<byte[]> response)
                : base(response.DebugInformation, response.OriginalException)
            {
                IsRetriable = !response.SuccessOrKnownError;
            }

            public bool IsRetriable { get; }
        }
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using Elasticsearch.Net;
using Vostok.Logging;

namespace Vostok.AirlockConsumer.Logs
{
    internal class LogEventMessageProcessor : IMessageProcessor<LogEventData>
    {
        private readonly ElasticLowLevelClient elasticClient;
        private readonly ILog log;

        public LogEventMessageProcessor(IEnumerable<string> elasticUriList)
        {
            log = Program.Log.ForContext<LogEventMessageProcessor>();
            var connectionPool = new StickyConnectionPool(elasticUriList.Select(x => new Uri(x)), null);
            var elasticConfig = new ConnectionConfiguration(connectionPool);
            elasticClient = new ElasticLowLevelClient(elasticConfig);
        }

        public void Process(IEnumerable<ConsumerEvent<LogEventData>> events)
        {
            log.Info("Process events");
            var elasticRecords = new List<object>();
            foreach (var consumerEvent in events)
            {
                elasticRecords.Add(new { index = new
                {
                    _index = ".kibana",
                    _type = "LogEvent"
                } });

                var logEventData = consumerEvent.Event;

                var elasticLogEvent = new Dictionary<string, string>(StringComparer.InvariantCultureIgnoreCase)
                {
                    ["@timestamp"] = logEventData.Timestamp.ToString("O"),
                    ["Level"] = logEventData.LogLevel,
                };

                if (!string.IsNullOrEmpty(logEventData.MessageTemplate))
                {
                    elasticLogEvent.Add("MessageTemplate", logEventData.MessageTemplate);
                }

                if (!string.IsNullOrEmpty(logEventData.Exception))
                {
                    elasticLogEvent.Add("Exception", logEventData.Exception);
                }

                foreach (var kvp in logEventData.Properties)
                {
                    if (elasticLogEvent.ContainsKey(kvp.Key))
                    {
                        continue;
                    }

                    elasticLogEvent.Add(kvp.Key, kvp.Value);
                }

                elasticRecords.Add(elasticLogEvent);
            }
            log.Info($"Send {elasticRecords.Count/2} events");
            var response = elasticClient.Bulk<byte[]>(new PostData<object>(elasticRecords));
            if (response.HttpStatusCode != (int)HttpStatusCode.OK)
            {
                log.Error($"Elasic error. code= {response.HttpStatusCode}, reason: {response.ServerError?.Error?.Reason}");
            }
        }
    }
}
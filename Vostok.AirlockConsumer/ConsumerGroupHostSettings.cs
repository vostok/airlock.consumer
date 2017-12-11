using System;
using System.Collections.Generic;
using System.Net;

namespace Vostok.AirlockConsumer
{
    public class ConsumerGroupHostSettings
    {
        public ConsumerGroupHostSettings(string bootstrapServers, string consumerGroupId, ProcessorHostSettings processorHostSettings, string autoResetOffsetPolicy, string clientId = null)
        {
            BootstrapServers = bootstrapServers;
            ConsumerGroupId = consumerGroupId;
            ProcessorHostSettings = processorHostSettings;
            ClientId = clientId ?? $"airlock@{Dns.GetHostName()}";
            AutoResetOffset = autoResetOffsetPolicy;
        }

        public string BootstrapServers { get; }
        public string ConsumerGroupId { get; }
        public ProcessorHostSettings ProcessorHostSettings { get; }
        public string ClientId { get; }
        public string AutoResetOffset { get; }
        public TimeSpan PollingInterval { get; } = TimeSpan.FromMilliseconds(100);
        public TimeSpan UpdateSubscriptionInterval { get; } = TimeSpan.FromSeconds(30);
        public TimeSpan UpdateSubscriptionTimeout { get; } = TimeSpan.FromSeconds(10);
        public TimeSpan FlushMetricsInterval { get; } = TimeSpan.FromMinutes(1);
        public TimeSpan OffsetsForTimesTimeout { get; } = TimeSpan.FromSeconds(10);
        public string ConsumerGroupHostId => $"{ConsumerGroupId}-{ClientId}";

        public Dictionary<string, object> GetConsumerConfig()
        {
            return new Dictionary<string, object>
            {
                {"bootstrap.servers", BootstrapServers},
                {"group.id", ConsumerGroupId},
                {"client.id", ClientId},
                {"api.version.request", true},
                {"api.version.request.timeout.ms", 10000},
                {"enable.auto.commit", false},
                {"enable.auto.offset.store", false},
                {"offset.store.method", "broker"},
                {"session.timeout.ms", 10000},
                {"heartbeat.interval.ms", 3000},
                {"socket.timeout.ms", 60000},
                {"statistics.interval.ms", 300000},
                {"topic.metadata.refresh.interval.ms", 300000},
                {"metadata.request.timeout.ms", 60000},
                {"partition.assignment.strategy", "roundrobin"},
                {"enable.partition.eof", true},
                {"check.crcs", false},
                {"fetch.min.bytes", 1},
                {"max.partition.fetch.bytes", 1048576},
                {"fetch.wait.max.ms", 100},
                {"queued.min.messages", 100000},
                {"queued.max.messages.kbytes", 1000000},
                {"receive.message.max.bytes", 100000000},
                {"max.in.flight.requests.per.connection", 1000000},
                { "default.topic.config", new Dictionary<string, object>
                    {
                        { "auto.offset.reset", AutoResetOffset }
                    }
                }
            };
        }
    }
}
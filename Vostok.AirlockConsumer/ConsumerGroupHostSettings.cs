using System;
using System.Collections.Generic;
using System.Net;

namespace Vostok.AirlockConsumer
{
    public class ConsumerGroupHostSettings
    {
        public ConsumerGroupHostSettings(string bootstrapServers, string consumerGroupId, string clientId = null, AutoResetOffsetPolicy autoResetOffsetPolicy = AutoResetOffsetPolicy.Latest)
        {
            BootstrapServers = bootstrapServers;
            ConsumerGroupId = consumerGroupId;
            ClientId = clientId ?? $"airlock@{Dns.GetHostName()}";
            AutoResetOffsetPolicy = autoResetOffsetPolicy;
        }

        public string BootstrapServers { get; }
        public string ConsumerGroupId { get; }
        public string ClientId { get; }
        public AutoResetOffsetPolicy AutoResetOffsetPolicy { get; }
        public TimeSpan PollingInterval { get; } = TimeSpan.FromMilliseconds(100);
        public string ConsumerGroupHostId => $"{ConsumerGroupId}-{ClientId}";

        public Dictionary<string, object> GetConsumerConfig()
        {
            return new Dictionary<string, object>
            {
                {"bootstrap.servers", BootstrapServers},
                {"group.id", ConsumerGroupId},
                {"client.id", ClientId},
                {"api.version.request", true},
                {"enable.auto.commit", false},
                {"enable.auto.offset.store", false},
                {"auto.offset.reset", FormatAutoResetOffsetPolicy(AutoResetOffsetPolicy)},
                {"session.timeout.ms", 30000},
                {"heartbeat.interval.ms", 1000},
                {"statistics.interval.ms", 300000},
                {"topic.metadata.refresh.interval.ms", 300000},
                {"partition.assignment.strategy", "roundrobin"},
                {"enable.partition.eof", true},
                {"check.crcs", false},
                {"offset.store.method", "broker"},
                {"fetch.min.bytes", 1},
                {"max.partition.fetch.bytes", 1048576},
                {"fetch.wait.max.ms", 100},
                {"queued.min.messages", 100000},
                {"queued.max.messages.kbytes", 1000000},
                {"receive.message.max.bytes", 100000000},
                {"max.in.flight.requests.per.connection", 1000000},
            };
        }

        private static string FormatAutoResetOffsetPolicy(AutoResetOffsetPolicy autoResetOffsetPolicy)
        {
            switch (autoResetOffsetPolicy)
            {
                case AutoResetOffsetPolicy.Latest:
                    return "latest";
                case AutoResetOffsetPolicy.Earliest:
                    return "earliest";
                default:
                    throw new ArgumentOutOfRangeException(nameof(autoResetOffsetPolicy), autoResetOffsetPolicy, null);
            }
        }
    }
}
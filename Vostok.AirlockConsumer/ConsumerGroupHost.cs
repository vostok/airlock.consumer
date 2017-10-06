using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using Vostok.Logging;

namespace Vostok.AirlockConsumer
{
    // seems like we should run counsumer groups on a per project
    public class ConsumerGroupHost : IDisposable
    {
        private readonly ConsumerGroupHostSettings settings;
        private readonly IAirlockEventProcessorProvider processorProvider;
        private readonly ILog log;
        private readonly Consumer<Null, byte[]> consumer;
        private readonly Dictionary<string, ProcessorHost> processorHosts = new Dictionary<string, ProcessorHost>();
        private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        private volatile Thread pollingThread;

        public ConsumerGroupHost(ConsumerGroupHostSettings settings, ILog log, IAirlockEventProcessorProvider processorProvider)
        {
            this.settings = settings;
            this.processorProvider = processorProvider;
            this.log = log.ForContext(this);

            consumer = new Consumer<Null, byte[]>(settings.GetConsumerConfig(), keyDeserializer: null, valueDeserializer: new ByteArrayDeserializer());
            consumer.OnError += (_, error) => { log.Error($"CriticalError: {error.ToString()}"); };
            consumer.OnConsumeError += (_, message) => { log.Error($"ConsumeError: from topic/partition/offset/timestamp {message.Topic}/{message.Partition}/{message.Offset}/{message.Timestamp.UtcDateTime:O}: {message.Error.ToString()}"); };
            consumer.OnLog += (_, logMessage) =>
            {
                LogLevel logLevel;
                switch (logMessage.Level)
                {
                    case 0:
                    case 1:
                    case 2:
                        logLevel = LogLevel.Fatal;
                        break;
                    case 3:
                        logLevel = LogLevel.Error;
                        break;
                    case 4:
                        logLevel = LogLevel.Warn;
                        break;
                    case 5:
                    case 6:
                        logLevel = LogLevel.Info;
                        break;
                    default:
                        logLevel = LogLevel.Debug;
                        break;
                }
                log.Log(logLevel, null, $"{logMessage.Name}|{logMessage.Facility}| {logMessage.Message}");
            };
            consumer.OnStatistics += (_, statJson) => { log.Info($"Statistics: {statJson}"); };
            consumer.OnPartitionEOF += (_, topicPartitionOffset) => { log.Info($"Reached end of topic/partition {topicPartitionOffset.Topic}/{topicPartitionOffset.Partition}, next message will be at offset {topicPartitionOffset.Offset}"); };
            consumer.OnOffsetsCommitted += (_, committedOffsets) =>
            {
                if (!committedOffsets.Error)
                    log.Info($"Successfully committed offsets: [{string.Join(", ", committedOffsets.Offsets)}]");
                else
                    log.Error($"Failed to commit offsets [{string.Join(", ", committedOffsets.Offsets)}]: {committedOffsets.Error}");
            };
            consumer.OnPartitionsAssigned += (_, topicPartitions) =>
            {
                // todo (avk, 04.10.2017): иногда нужно процессоры создавать динамически при перебалансировке партишенов по консьюмерам
                log.Warn($"PartitionsAssigned: consumer.Name: {consumer.Name}, consumer.MemberId: {consumer.MemberId}, topicPartitions: [{string.Join(", ", topicPartitions)}]");
                consumer.Assign(topicPartitions.Select(x => new TopicPartitionOffset(x, Offset.Invalid)));
            };
            consumer.OnPartitionsRevoked += (_, topicPartitions) =>
            {
                // todo (avk, 04.10.2017): иногда нужно процессоры создавать динамически при перебалансировке партишенов по консьюмерам
                log.Warn($"PartitionsRevoked: [{string.Join(", ", topicPartitions)}]");
                consumer.Unassign();
            };
            consumer.OnMessage += (_, message) => OnMessage(message);
        }

        public void Start()
        {
            if (pollingThread != null)
                throw new InvalidOperationException("ConsumerGroupHost has been already run");
            Subscribe();
            pollingThread = new Thread(PollingThreadFunc)
            {
                IsBackground = true,
                Name = $"poll-{settings.ConsumerGroupHostId}",
            };
            pollingThread.Start();
        }

        public void Stop()
        {
            if (pollingThread == null)
                throw new InvalidOperationException("ConsumerGroupHost is not started");
            Dispose();
        }

        public void Dispose()
        {
            if (pollingThread == null)
                return;
            cancellationTokenSource.Cancel();
            pollingThread.Join();
            consumer.Dispose();
            cancellationTokenSource.Dispose();
        }

        // todo (avk, 04.10.2017): периодически нужно подписыватьс€ на по€вл€ющиес€ топики
        private void Subscribe()
        {
            var metadata = consumer.GetMetadata(allTopics: true);
            log.Debug($"Metadata: {metadata}");
            var topicsToSubscribeTo = new List<string>();
            foreach (var topicMetadata in metadata.Topics)
            {
                var routingKey = topicMetadata.Topic;
                var processor = processorProvider.TryGetProcessor(routingKey);
                if (processor != null)
                {
                    topicsToSubscribeTo.Add(routingKey);
                    var processorHost = new ProcessorHost(settings.ConsumerGroupHostId, routingKey, cancellationTokenSource.Token, processor, log);
                    processorHosts.Add(routingKey, processorHost);
                    processorHost.Start();
                }
            }
            log.Warn($"TopicsToSubscribeTo: [{string.Join(", ", topicsToSubscribeTo)}]");
            if (!topicsToSubscribeTo.Any())
                throw new InvalidOperationException("No topics to subscribe to");
            consumer.Subscribe(topicsToSubscribeTo);
        }

        private void PollingThreadFunc()
        {
            try
            {
                while (!cancellationTokenSource.IsCancellationRequested)
                    consumer.Poll(settings.PollingInterval);
                foreach (var processorHost in processorHosts.Values)
                    processorHost.CompleteAdding();
                foreach (var processorHost in processorHosts.Values)
                    processorHost.Stop();
            }
            catch (Exception e)
            {
                log.Fatal("Unhandled exception on polling thread", e);
                throw;
            }
        }

        private void OnMessage(Message<Null, byte[]> message)
        {
            if (!processorHosts.TryGetValue(message.Topic, out var processorHost))
                throw new InvalidOperationException($"Invalid routingKey: {message.Topic}");

            // todo (avk, 04.10.2017): не блокироватьс€ из-за неуспевающих обработчиков (use consumer.Pause() api)
            processorHost.Enqueue(message);
        }
    }
}
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Confluent.Kafka;
using Vostok.AirlockConsumer.Deserialization;
using Vostok.Logging;

namespace Vostok.AirlockConsumer
{
    public class ConsumerGroupHost : IDisposable
    {
        private readonly string consumerGroupId;
        private readonly string clientId;
        private readonly IAirlockEventProcessorProvider processorProvider;
        private readonly ILog log;
        private readonly Consumer<Null, byte[]> consumer;
        private readonly Dictionary<string, ProcessorInfo> processorInfos = new Dictionary<string, ProcessorInfo>();
        private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        private readonly TimeSpan kafkaConsumerPollingInterval = TimeSpan.FromMilliseconds(100);
        private volatile Thread pollingThread;

        public ConsumerGroupHost(string kafkaBootstrapEndpoints, string consumerGroupId, string clientId, bool enableAutoCommit, ILog log, IAirlockEventProcessorProvider processorProvider)
        {
            this.consumerGroupId = consumerGroupId;
            this.clientId = clientId;
            this.processorProvider = processorProvider;
            this.log = log.ForContext(this);

            var config = GetConsumerConfig(kafkaBootstrapEndpoints, consumerGroupId, clientId, enableAutoCommit);
            consumer = new Consumer<Null, byte[]>(config, keyDeserializer: null, valueDeserializer: new ByteArrayDeserializer());
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
                log.Info($"PartitionsAssigned: consumer.Name: {consumer.Name}, consumer.MemberId: {consumer.MemberId}, topicPartitions: [{string.Join(", ", topicPartitions)}]");
                consumer.Assign(topicPartitions.Select(x => new TopicPartitionOffset(x, Offset.Invalid)));
            };
            consumer.OnPartitionsRevoked += (_, topicPartitions) =>
            {
                // todo (avk, 04.10.2017): иногда нужно процессоры создавать динамически при перебалансировке партишенов по консьюмерам
                log.Info($"PartitionsRevoked: [{string.Join(", ", topicPartitions)}]");
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
                Name = $"poll-{consumerGroupId}-{clientId}",
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
            log.Info($"Metadata: {metadata}");
            var topicsToSubscribeTo = new List<string>();
            foreach (var topicMetadata in metadata.Topics)
            {
                var routingKey = topicMetadata.Topic;
                var processor = processorProvider.TryGetProcessor(routingKey);
                if (processor != null)
                {
                    topicsToSubscribeTo.Add(routingKey);
                    var processorThread = new Thread(ProcessorThreadFunc)
                    {
                        IsBackground = true,
                        Name = $"processor-{consumerGroupId}-{clientId}-{processor.ProcessorId}",
                    };
                    var processorInfo = new ProcessorInfo(routingKey, processor, processorThread);
                    processorInfos.Add(routingKey, processorInfo);
                    processorThread.Start(processorInfo);
                }
            }
            log.Info($"TopicsToSubscribeTo: [{string.Join(", ", topicsToSubscribeTo)}]");
            if (!topicsToSubscribeTo.Any())
                throw new InvalidOperationException("No topics to subscribe to");
            consumer.Subscribe(topicsToSubscribeTo);
        }

        private void PollingThreadFunc()
        {
            try
            {
                while (!cancellationTokenSource.IsCancellationRequested)
                    consumer.Poll(kafkaConsumerPollingInterval);
                foreach (var processorInfo in processorInfos.Values)
                    processorInfo.EventsQueue.CompleteAdding();
                foreach (var processorInfo in processorInfos.Values)
                    processorInfo.ProcessorThread.Join();
            }
            catch (Exception e)
            {
                log.Fatal("Unhandled exception on polling thread", e);
                throw;
            }
        }

        private void ProcessorThreadFunc(object arg)
        {
            var processorInfo = (ProcessorInfo)arg;
            try
            {
                while (!processorInfo.EventsQueue.IsCompleted)
                {
                    var messageBatch = DequeueMessageBatch(processorInfo);
                    if (messageBatch.Count > 0)
                        ProcessMessageBatch(processorInfo, messageBatch);
                }
            }
            catch (Exception e)
            {
                log.Fatal("Unhandled exception on processor thread", e);
                throw;
            }
        }

        private List<Message<Null, byte[]>> DequeueMessageBatch(ProcessorInfo processorInfo)
        {
            var sw = Stopwatch.StartNew();
            var messageBatch = new List<Message<Null, byte[]>>();
            var dequeueTimeout = processorInfo.MaxDequeueTimeout;
            while (true)
            {
                if (processorInfo.EventsQueue.TryTake(out var message, dequeueTimeout))
                {
                    messageBatch.Add(message);
                    if (messageBatch.Count >= processorInfo.MaxBatchSize)
                        break;
                }
                else
                {
                    if (cancellationTokenSource.IsCancellationRequested)
                    {
                        if (processorInfo.EventsQueue.IsAddingCompleted)
                            break;
                        dequeueTimeout = TimeSpan.Zero;
                    }
                    else
                    {
                        var elapsed = sw.Elapsed;
                        if (elapsed > processorInfo.MaxDequeueTimeout)
                            break;
                        dequeueTimeout = processorInfo.MaxDequeueTimeout - elapsed;
                        if (dequeueTimeout < processorInfo.MinDequeueTimeout)
                            dequeueTimeout = processorInfo.MinDequeueTimeout;
                    }
                }
            }
            return messageBatch;
        }

        private void ProcessMessageBatch(ProcessorInfo processorInfo, List<Message<Null, byte[]>> messageBatch)
        {
            var airlockEvents = messageBatch.Select(x => new AirlockEvent<byte[]>
            {
                RoutingKey = processorInfo.RoutingKey,
                Timestamp = x.Timestamp.UtcDateTime,
                Payload = x.Value,
            }).ToList();
            try
            {
                processorInfo.Processor.ProcessAsync(airlockEvents).GetAwaiter().GetResult();
            }
            catch (Exception e)
            {
                log.Error($"Processor failed: {processorInfo}", e);
            }
        }

        private void OnMessage(Message<Null, byte[]> message)
        {
            if (!processorInfos.TryGetValue(message.Topic, out var processorInfo))
                throw new InvalidOperationException($"Invalid routingKey: {message.Topic}");

            // todo (avk, 04.10.2017): не блокироватьс€ из-за неуспевающих обработчиков (use consumer.Pause() api)
            processorInfo.EventsQueue.Add(message);
        }

        private static Dictionary<string, object> GetConsumerConfig(string kafkaBootstrapEndpoints, string consumerGroupId, string clientId, bool enableAutoCommit)
        {
            return new Dictionary<string, object>
            {
                {"bootstrap.servers", kafkaBootstrapEndpoints},
                {"group.id", consumerGroupId},
                {"client.id", clientId},
                {"enable.auto.commit", enableAutoCommit},
                {"auto.commit.interval.ms", 1000},
                {"auto.offset.reset", "latest"},
                {"session.timeout.ms", 60000},
                {"statistics.interval.ms", 60000},

                //{"max.poll.records", 100*1000},
                //{"max.partition.fetch.bytes", 10485760},
                //{"fetch.min.bytes", 1},
                //{"fetch.max.bytes", 52428800},
                //{"fetch.max.wait.ms", 500},
                {"queued.min.messages", 2000000},
                {"queued.max.messages.kbytes", 50050000},
                {"fetch.message.max.bytes", 1000000},
                {"fetch.wait.max.ms", 500},
                {"receive.message.max.bytes", 52500000},
                {"max.in.flight.requests.per.connection", 500000},
            };
        }

        private class ProcessorInfo
        {
            private const int MaxProcessorQueueSize = int.MaxValue;

            public ProcessorInfo(string routingKey, IAirlockEventProcessor processor, Thread processorThread)
            {
                RoutingKey = routingKey;
                Processor = processor;
                ProcessorThread = processorThread;
            }

            public string RoutingKey { get; }
            public Thread ProcessorThread { get; }
            public IAirlockEventProcessor Processor { get; }
            public BoundedBlockingQueue<Message<Null, byte[]>> EventsQueue { get; } = new BoundedBlockingQueue<Message<Null, byte[]>>(MaxProcessorQueueSize);
            public int MaxBatchSize { get; } = 100*1000;
            public TimeSpan MaxDequeueTimeout { get; } = TimeSpan.FromSeconds(1);
            public TimeSpan MinDequeueTimeout { get; } = TimeSpan.FromMilliseconds(100);

            public override string ToString()
            {
                return $"RoutingKey: {RoutingKey}, ProcessorType: {Processor.GetType().Name}, ProcessorId: {Processor.ProcessorId}";
            }
        }
    }
}
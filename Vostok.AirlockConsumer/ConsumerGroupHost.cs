using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using Vostok.Logging;

namespace Vostok.AirlockConsumer
{
    public class ProcessorInfo
    {
        public IAirlockEventProcessor Processor { get; set; }
        public ProcessorHost ProcessorHost { get; set; }
        public int[] AssignedPartitions { get; set; }
        public bool IsPaused { get; set; }
    }

    // todo (avk, 09.10.2017): integration tests for airlock consumer machinery https://github.com/vostok/airlock.consumer/issues/4
    // todo (avk, 06.10.2017): handle kafka consumer exceptions (introduce decorator) https://github.com/vostok/airlock.consumer/issues/19
    public class ConsumerGroupHost : IDisposable
    {
        private readonly ConsumerGroupHostSettings settings;
        private readonly ILog log;
        private readonly IAirlockEventProcessorProvider processorProvider;
        private readonly IRoutingKeyFilter routingKeyFilter;
        private readonly Consumer<Null, byte[]> consumer;
        private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        private readonly Dictionary<string, ProcessorInfo> processorInfos = new Dictionary<string, ProcessorInfo>();
        private readonly Dictionary<string, ProcessorInfo> pausedProcessorInfos = new Dictionary<string, ProcessorInfo>();
        private HashSet<string> topicsAlreadySubscribedTo = new HashSet<string>();
        private volatile Thread pollingThread;

        public ConsumerGroupHost(ConsumerGroupHostSettings settings, ILog log, IRoutingKeyFilter routingKeyFilter, IAirlockEventProcessorProvider processorProvider)
        {
            this.settings = settings;
            this.log = log;
            this.routingKeyFilter = routingKeyFilter;
            this.processorProvider = processorProvider;

            consumer = new Consumer<Null, byte[]>(settings.GetConsumerConfig(), keyDeserializer: null, valueDeserializer: new ByteArrayDeserializer());
            consumer.OnError += (_, error) => { log.Error($"CriticalError: consumerName: {consumer.Name}, memberId: {consumer.MemberId} - {error.ToString()}"); };
            consumer.OnConsumeError += (_, message) => { log.Error($"ConsumeError: consumerName: {consumer.Name}, memberId: {consumer.MemberId}, topic: {message.Topic}, partition: {message.Partition}, offset: {message.Offset}, timestamp: {message.Timestamp.UtcDateTime:O}: {message.Error.ToString()}"); };
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
                log.Log(logLevel, null, $"consumerName: {consumer.Name}, memberId: {consumer.MemberId} - {logMessage.Name}|{logMessage.Facility}| {logMessage.Message}");
            };
            consumer.OnStatistics += (_, statJson) => { log.Debug($"Statistics: consumerName: {consumer.Name}, memberId: {consumer.MemberId}, stat: {statJson}"); };
            consumer.OnPartitionEOF += (_, topicPartitionOffset) => { log.Debug($"PartitionEof: consumerName: {consumer.Name}, memberId: {consumer.MemberId}, topicPartition: {topicPartitionOffset.TopicPartition}, next message will be at offset {topicPartitionOffset.Offset}"); };
            consumer.OnOffsetsCommitted += (_, committedOffsets) =>
            {
                if (!committedOffsets.Error)
                    log.Debug($"OffsetsCommitted: consumerName: {consumer.Name}, memberId: {consumer.MemberId} successfully committed offsets: [{string.Join(", ", committedOffsets.Offsets)}]");
                else
                    log.Error($"OffsetsCommitted: consumerName: {consumer.Name}, memberId: {consumer.MemberId} failed to commit offsets [{string.Join(", ", committedOffsets.Offsets)}]: {committedOffsets.Error}");
            };
            consumer.OnPartitionsRevoked += (_, topicPartitions) => { OnPartitionsRevoked(topicPartitions); };
            consumer.OnPartitionsAssigned += (_, topicPartitions) => { OnPartitionsAssigned(topicPartitions); };
            consumer.OnMessage += (_, message) => OnMessage(message);
        }

        public void Start()
        {
            if (pollingThread != null)
                throw new InvalidOperationException("ConsumerGroupHost has been already run");
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

        private void PollingThreadFunc()
        {
            try
            {
                var subscribedToAnything = UpdateSubscription();
                var swUpdateSubscription = Stopwatch.StartNew();
                var swCheckPaused = Stopwatch.StartNew();
                while (!cancellationTokenSource.IsCancellationRequested)
                {
                    if (subscribedToAnything)
                        consumer.Poll(settings.PollingInterval);
                    else
                        Thread.Sleep(settings.PollingInterval);

                    if (swUpdateSubscription.Elapsed > settings.UpdateSubscriptionInterval)
                    {
                        subscribedToAnything = UpdateSubscription();
                        swUpdateSubscription.Restart();
                    }
                    if (swCheckPaused.Elapsed > settings.CheckPausedInterval)
                    {
                        if (pausedProcessorInfos.Count > 0)
                            CheckPaused();
                        swCheckPaused.Restart();
                    }
                }
                Unsubscribe();
                // todo (avk, 08.10.2017, maybe): wait for consumer to finish outstanding fetching operations and process already fetched events https://github.com/vostok/airlock.consumer/issues/20
                StopProcessors(processorInfos.Values.Select(x => x.ProcessorHost).ToList());
            }
            catch (Exception e)
            {
                log.Fatal("Unhandled exception on polling thread", e);
                throw;
            }
        }

        private void Unsubscribe()
        {
            consumer.Unsubscribe();
            log.Info($"Unsubscribe: consumerName: {consumer.Name}, memberId: {consumer.MemberId} unsubscribed from all topics");
        }

        private static void StopProcessors(List<ProcessorHost> processorHostsToStop)
        {
            foreach (var processorHost in processorHostsToStop)
                processorHost.CompleteAdding();
            foreach (var processorHost in processorHostsToStop)
                processorHost.WaitForTermination();
        }

        private bool UpdateSubscription()
        {
            var metadata = consumer.GetMetadata(allTopics: true);
            log.Debug($"GotMetadata: consumerName: {consumer.Name}, memberId: {consumer.MemberId}, metadata: {metadata}");

            var topicsToSubscribeTo = new HashSet<string>();
            foreach (var topicMetadata in metadata.Topics)
            {
                var routingKey = topicMetadata.Topic;
                if (routingKeyFilter.Matches(routingKey))
                    topicsToSubscribeTo.Add(routingKey);
            }

            if (!topicsToSubscribeTo.Any())
            {
                if (topicsAlreadySubscribedTo.Any())
                {
                    Unsubscribe();
                    topicsAlreadySubscribedTo.Clear();
                }
                return false;
            }

            if (!topicsToSubscribeTo.SetEquals(topicsAlreadySubscribedTo))
            {
                consumer.Subscribe(topicsToSubscribeTo);
                topicsAlreadySubscribedTo = topicsToSubscribeTo;
                log.Info($"SubscribedToTopics: consumerName: {consumer.Name}, memberId: {consumer.MemberId}, topics: [{string.Join(", ", topicsToSubscribeTo)}]");
            }
            return true;
        }

        private void OnPartitionsAssigned(List<TopicPartition> topicPartitions)
        {
            log.Debug($"PartitionsAssignmentRequest: consumerName: {consumer.Name}, memberId: {consumer.MemberId}, topicPartitions: [{string.Join(", ", topicPartitions)}]");
            var topicPartitionOffsets = HandlePartitionsAssignment(topicPartitions);
            consumer.Assign(topicPartitionOffsets);
            log.Info($"PartitionsAssigned: consumerName: {consumer.Name}, memberId: {consumer.MemberId}, topicPartitions: [{string.Join(", ", topicPartitionOffsets)}]");
        }

        private void OnPartitionsRevoked(List<TopicPartition> topicPartitions)
        {
            log.Debug($"PartitionsRevokationRequest: consumerName: {consumer.Name}, memberId: {consumer.MemberId}, topicPartitions: [{string.Join(", ", topicPartitions)}]");
            consumer.Unassign();
            HandlePartitionsAssignment(new List<TopicPartition>());
            log.Info($"PartitionsRevoked: consumerName: {consumer.Name}, memberId: {consumer.MemberId}, topicPartitions: [{string.Join(", ", topicPartitions)}]");
        }

        private List<TopicPartitionOffset> HandlePartitionsAssignment(List<TopicPartition> topicPartitions)
        {
            var routingKeysToAssign = new List<string>();
            var topicPartitionOffsets = new List<TopicPartitionOffset>();
            foreach (var topicPartitionsByRoutingKeyGroup in topicPartitions.GroupBy(x => x.Topic))
            {
                var routingKey = topicPartitionsByRoutingKeyGroup.Key;
                routingKeysToAssign.Add(routingKey);
                if (!processorInfos.TryGetValue(routingKey, out var processorInfo))
                {
                    var processor = processorProvider.GetProcessor(routingKey);
                    var processorHost = new ProcessorHost(settings.ConsumerGroupHostId, routingKey, cancellationTokenSource.Token, processor, log, consumer);
                    processorInfo = new ProcessorInfo
                    {
                        Processor = processor,
                        ProcessorHost = processorHost,
                        AssignedPartitions = new int[0]
                    };
                    processorInfos.Add(routingKey, processorInfo);
                    processorHost.Start();
                }

                var partitionsToAssign = topicPartitionsByRoutingKeyGroup.Select(x => x.Partition).ToArray();
                var newPartitions = partitionsToAssign.Except(processorInfo.AssignedPartitions).ToList();
                if (newPartitions.Any())
                {
                    var startTimestampOnRebalance = processorInfo.Processor.GetStartTimestampOnRebalance(routingKey);
                    if (!startTimestampOnRebalance.HasValue)
                        topicPartitionOffsets.AddRange(newPartitions.Select(x => new TopicPartitionOffset(routingKey, x, Offset.Invalid)));
                    else
                    {
                        var timestampToSearch = new Timestamp(startTimestampOnRebalance.Value.ToUnixTimeMilliseconds(), TimestampType.NotAvailable);
                        var timestampsToSearch = newPartitions.Select(x => new TopicPartitionTimestamp(routingKey, x, timestampToSearch));
                        var offsetsForTimes = consumer.OffsetsForTimes(timestampsToSearch, settings.OffsetsForTimesTimeout);
                        foreach (var topicPartitionOffsetError in offsetsForTimes)
                        {
                            if (!topicPartitionOffsetError.Error)
                            {
                                var offset = topicPartitionOffsetError.Offset;
                                if (offset == timestampToSearch.UnixTimestampMs)
                                {
                                    offset = Offset.Invalid;
                                    log.Error($"consumerName: {consumer.Name}, memberId: {consumer.MemberId} failed to get offset for timestamp: timestampToSearch ({timestampToSearch.UnixTimestampMs}) == offset for: {topicPartitionOffsetError}");
                                }
                                topicPartitionOffsets.Add(new TopicPartitionOffset(topicPartitionOffsetError.TopicPartition, offset));
                            }
                            else
                            {
                                log.Error($"consumerName: {consumer.Name}, memberId: {consumer.MemberId}, failed to get offset for timestamp {startTimestampOnRebalance}: {topicPartitionOffsetError}");
                                topicPartitionOffsets.Add(new TopicPartitionOffset(topicPartitionOffsetError.TopicPartition, Offset.Invalid));
                            }
                        }
                    }
                }
                var remainingPartitions = partitionsToAssign.Except(topicPartitionOffsets.Select(x => x.Partition));
                topicPartitionOffsets.AddRange(remainingPartitions.Select(x => new TopicPartitionOffset(routingKey, x, Offset.Invalid)));
                processorInfo.AssignedPartitions = partitionsToAssign;
            }

            var processorHostsToStop = new List<ProcessorHost>();
            var routingKeysToUnassign = processorInfos.Keys.Except(routingKeysToAssign);
            foreach (var routingKey in routingKeysToUnassign)
            {
                processorHostsToStop.Add(processorInfos[routingKey].ProcessorHost);
                processorInfos.Remove(routingKey);
                if (pausedProcessorInfos.ContainsKey(routingKey))
                    pausedProcessorInfos.Remove(routingKey);
            }
            if (processorHostsToStop.Count > 0)
                StopProcessors(processorHostsToStop);

            if(topicPartitionOffsets.Count != topicPartitions.Count)
                throw new InvalidOperationException($"AssertionFailre: topicPartitionOffsets.Count ({topicPartitionOffsets.Count}) != topicPartitions.Count {topicPartitions.Count}");
            return topicPartitionOffsets;
        }

        private void CheckPaused()
        {
            foreach (var kvProcessorInfo in pausedProcessorInfos)
            {
                var processorInfo = kvProcessorInfo.Value;
                if (!processorInfo.ProcessorHost.IsOverflow())
                {
                    consumer.Resume(processorInfo.AssignedPartitions.Select(p => new TopicPartition(kvProcessorInfo.Key, p)));
                    processorInfo.IsPaused = false;
                }
            }
        }

        private void OnMessage(Message<Null, byte[]> message)
        {
            if (!processorInfos.TryGetValue(message.Topic, out var processorInfo))
                throw new InvalidOperationException($"Invalid routingKey: {message.Topic}");
            var processorHost = processorInfo.ProcessorHost;
            if (processorHost.IsOverflow() && !processorInfo.IsPaused)
            {
                consumer.Pause(processorInfo.AssignedPartitions.Select(p => new TopicPartition(message.Topic, p)));
                processorInfo.IsPaused = true;
                pausedProcessorInfos.Add(message.Topic, processorInfo);
            }
            processorHost.Enqueue(message);
        }
    }
}
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using Vostok.Logging;
using Vostok.Metrics;

namespace Vostok.AirlockConsumer
{
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
        private readonly Dictionary<string, (IAirlockEventProcessor Processor, ProcessorHost ProcessorHost)> processorInfos = new Dictionary<string, (IAirlockEventProcessor Processor, ProcessorHost)>();
        private readonly ConsumerMetrics metrics;
        private HashSet<string> topicsAlreadySubscribedTo = new HashSet<string>();
        private volatile Thread pollingThread;

        public ConsumerGroupHost(ConsumerGroupHostSettings settings, ILog log, IMetricScope rootMetricScope, IRoutingKeyFilter routingKeyFilter, IAirlockEventProcessorProvider processorProvider)
        {
            this.settings = settings;
            this.log = log;
            this.routingKeyFilter = routingKeyFilter;
            this.processorProvider = processorProvider;
            metrics = new ConsumerMetrics(settings.FlushMetricsInterval, rootMetricScope);

            consumer = new Consumer<Null, byte[]>(settings.GetConsumerConfig(), keyDeserializer: null, valueDeserializer: new ByteArrayDeserializer());
            consumer.OnError += (_, error) =>
            {
                log.Error($"CriticalError: consumerName: {consumer.Name}, memberId: {consumer.MemberId} - {error.ToString()}");
                metrics.CriticalErrorCounter.Add();
            };
            consumer.OnConsumeError += (_, message) =>
            {
                log.Error($"ConsumeError: consumerName: {consumer.Name}, memberId: {consumer.MemberId}, topic: {message.Topic}, partition: {message.Partition}, offset: {message.Offset}, timestamp: {message.Timestamp.UtcDateTime:O}: {message.Error.ToString()}");
                metrics.ConsumeErrorCounter.Add();
            };
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

            consumer.OnStatistics += OnConsumerStatistics;
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
                var needToPoll = UpdateSubscription();
                var swUpdateSubscription = Stopwatch.StartNew();
                while (!cancellationTokenSource.IsCancellationRequested)
                {
                    if (needToPoll)
                        consumer.Poll(settings.PollingInterval);
                    else
                        Thread.Sleep(settings.PollingInterval);

                    if (swUpdateSubscription.Elapsed > settings.UpdateSubscriptionInterval)
                    {
                        needToPoll = UpdateSubscription();
                        foreach (var processorHost in processorInfos.Values.Select(x => x.ProcessorHost))
                        {
                            processorHost.TryResumeConsumption();
                        }
                        swUpdateSubscription.Restart();
                    }
                }
                Unsubscribe();
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
                processorHost.Stop();
            foreach (var processorHost in processorHostsToStop)
            {
                processorHost.WaitForTermination();
                processorHost.Dispose();
            }
        }

        private bool UpdateSubscription()
        {
            Metadata metadata;
            try
            {
                metadata = consumer.GetMetadata(allTopics: true, timeout: settings.UpdateSubscriptionTimeout);
            }
            catch (Exception e)
            {
                log.Error($"Could not get metadata, consumerName: {consumer.Name}, memberId: {consumer.MemberId}", e);
                return true;
            }
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
            log.Info($"PartitionsRevoked: consumerName: {consumer.Name}, memberId: {consumer.MemberId}, topicPartitions: [{string.Join(", ", topicPartitions)}]");
        }

        // todo (avk, 19.10.2017), rafactoring: move AssignedPartitions initialization logic into ProcessorHost
        private List<TopicPartitionOffset> HandlePartitionsAssignment(List<TopicPartition> topicPartitions)
        {
            var routingKeysToAssign = new List<string>();
            var topicPartitionOffsets = new List<TopicPartitionOffset>();
            foreach (var topicPartitionsByRoutingKeyGroup in topicPartitions.GroupBy(x => x.Topic))
            {
                var routingKey = topicPartitionsByRoutingKeyGroup.Key;
                log.Debug("Process topic " + routingKey);
                routingKeysToAssign.Add(routingKey);
                if (!processorInfos.TryGetValue(routingKey, out var processorInfo))
                {
                    var processor = processorProvider.GetProcessor(routingKey);
                    var processorHost = new ProcessorHost(settings.ConsumerGroupHostId, routingKey, processor, log, consumer, metrics.GetProcessorScope(routingKey), settings.FlushMetricsInterval, settings.ProcessorHostSettings);
                    processorInfo = (processor, processorHost);
                    processorInfos.Add(routingKey, processorInfo);
                    processorHost.Start();
                }

                var partitionsToAssign = topicPartitionsByRoutingKeyGroup.Select(x => x.Partition).ToArray();
                log.Debug("partitionsToAssign = " + string.Join(",", partitionsToAssign));

                var newPartitions = partitionsToAssign.Except(processorInfo.ProcessorHost.AssignedPartitions).ToList();
                log.Debug("newPartitions = " + string.Join(",", newPartitions));
                if (newPartitions.Any())
                {
                    var startTimestampOnRebalance = processorInfo.Processor.GetStartTimestampOnRebalance(routingKey);
                    if (!startTimestampOnRebalance.HasValue)
                    {
                        log.Debug("startTimestampOnRebalance is null");
                        topicPartitionOffsets.AddRange(newPartitions.Select(x => new TopicPartitionOffset(routingKey, x, Offset.Invalid)));
                    }
                    else
                    {
                        log.Debug("startTimestampOnRebalance is not null");
                        var timestampToSearch = new Timestamp(startTimestampOnRebalance.Value.ToUnixTimeMilliseconds(), TimestampType.NotAvailable);
                        var timestampsToSearch = newPartitions.Select(x => new TopicPartitionTimestamp(routingKey, x, timestampToSearch));
                        var offsetsForTimes = consumer.OffsetsForTimes(timestampsToSearch, settings.OffsetsForTimesTimeout);
                        foreach (var topicPartitionOffset in offsetsForTimes)
                        {
                            if (!topicPartitionOffset.Error)
                            {
                                var offset = topicPartitionOffset.Offset;
                                if (offset == timestampToSearch.UnixTimestampMs)
                                {
                                    offset = Offset.Invalid;
                                    log.Error($"consumerName: {consumer.Name}, memberId: {consumer.MemberId} failed to get offset for timestamp: timestampToSearch ({timestampToSearch.UnixTimestampMs}) == offset for: {topicPartitionOffset}");
                                }
                                log.Debug($"set offset {offset} for partition {topicPartitionOffset.TopicPartition}");
                                topicPartitionOffsets.Add(new TopicPartitionOffset(topicPartitionOffset.TopicPartition, offset));
                            }
                            else
                            {
                                log.Error($"consumerName: {consumer.Name}, memberId: {consumer.MemberId}, failed to get offset for timestamp {startTimestampOnRebalance}: {topicPartitionOffset}");
                                topicPartitionOffsets.Add(new TopicPartitionOffset(topicPartitionOffset.TopicPartition, Offset.Invalid));
                            }
                        }
                    }
                }
                var remainingPartitions = partitionsToAssign.Except(topicPartitionOffsets.Where(x => x.Topic == routingKey).Select(x => x.Partition)).ToArray();
                log.Debug("remainingPartitions = " + string.Join(",", remainingPartitions));

                topicPartitionOffsets.AddRange(remainingPartitions.Select(x => new TopicPartitionOffset(routingKey, x, Offset.Invalid)));
                processorInfo.ProcessorHost.AssignedPartitions = partitionsToAssign;
            }

            var processorHostsToStop = new List<ProcessorHost>();
            var routingKeysToUnassign = processorInfos.Keys.Except(routingKeysToAssign).ToList();
            foreach (var routingKey in routingKeysToUnassign)
            {
                processorHostsToStop.Add(processorInfos[routingKey].ProcessorHost);
                processorInfos.Remove(routingKey);
            }
            StopProcessors(processorHostsToStop);
            metrics.ProcessorCount = processorInfos.Count;

            if (topicPartitionOffsets.Count != topicPartitions.Count)
                throw new InvalidOperationException($"AssertionFailure: topicPartitionOffsets.Count ({topicPartitionOffsets.Count}) != topicPartitions.Count ({topicPartitions.Count})");
            return topicPartitionOffsets;
        }

        private void OnMessage(Message<Null, byte[]> message)
        {
            metrics.IncrementMessage();
            if (!processorInfos.TryGetValue(message.Topic, out var processorInfo))
                throw new InvalidOperationException($"Invalid routingKey: {message.Topic}");
            processorInfo.ProcessorHost.Enqueue(message);
        }

        private void OnConsumerStatistics(object _, string statJson)
        {
            metrics.UpdateKafkaStat(statJson);
            log.Debug($"Statistics: consumerName: {consumer.Name}, memberId: {consumer.MemberId}, stat: {statJson}");
        }
    }
}
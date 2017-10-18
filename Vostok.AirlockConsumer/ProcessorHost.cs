using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Confluent.Kafka;
using Vostok.Logging;

namespace Vostok.AirlockConsumer
{
    public class ProcessorHost
    {
        private const int maxBatchSize = 100*1000;
        private const int overflowLimit = maxBatchSize*10;
        private readonly string routingKey;
        private readonly CancellationToken stopSignal;
        private readonly IAirlockEventProcessor processor;
        private readonly ILog log;
        private readonly Consumer<Null, byte[]> consumer;
        private readonly TimeSpan maxDequeueTimeout = TimeSpan.FromSeconds(1);
        private readonly TimeSpan minDequeueTimeout = TimeSpan.FromMilliseconds(100);
        private readonly BlockingCollection<Message<Null, byte[]>> eventsQueue = new BlockingCollection<Message<Null, byte[]>>(new ConcurrentQueue<Message<Null, byte[]>>());
        private readonly Thread processorThread;
        private int[] pausedPartitions;

        public ProcessorHost(string consumerGroupHostId, string routingKey, CancellationToken stopSignal, IAirlockEventProcessor processor, ILog log, Consumer<Null, byte[]> consumer)
        {
            this.routingKey = routingKey;
            this.stopSignal = stopSignal;
            this.processor = processor;
            this.log = log;
            this.consumer = consumer;
            AssignedPartitions = new int[0];
            processorThread = new Thread(ProcessorThreadFunc)
            {
                IsBackground = true,
                Name = $"processor-{consumerGroupHostId}-{processor.ProcessorId}",
            };
        }

        public int[] AssignedPartitions { get; set; }

        public void Start()
        {
            processorThread.Start();
        }

        public void Enqueue(Message<Null, byte[]> message)
        {
            if (pausedPartitions != null)
                throw new InvalidOperationException($"ProcessorHost is paused for routingKey: {routingKey}");
            eventsQueue.Add(message, CancellationToken.None);
            if (eventsQueue.Count >= overflowLimit)
            {
                var partitionsToPause = AssignedPartitions.ToArray();
                consumer.Pause(partitionsToPause.Select(p => new TopicPartition(message.Topic, p)));
                log.Warn($"PausedConsumption: consumerName: {consumer.Name}, memberId: {consumer.MemberId}, routingKey: {message.Topic}, processorId: {processor.ProcessorId}, pausedPartitions: [{string.Join(", ", partitionsToPause)}]");
                pausedPartitions = partitionsToPause;
            }
        }

        public void TryResumeConsumption()
        {
            if (pausedPartitions == null)
                return;
            if (eventsQueue.Count > 0)
                return;
            var partitionsToResume = pausedPartitions.Intersect(AssignedPartitions).ToArray();
            if (!partitionsToResume.Any())
                log.Warn($"NothingToResume: consumerName: {consumer.Name}, memberId: {consumer.MemberId}, routingKey: {routingKey}, processorId: {processor.ProcessorId}");
            else
            {
                consumer.Resume(partitionsToResume.Select(partition => new TopicPartition(routingKey, partition)));
                log.Warn($"ResumedConsumption: consumerName: {consumer.Name}, memberId: {consumer.MemberId}, routingKey: {routingKey}, processorId: {processor.ProcessorId}, resumedPartitions: [{string.Join(", ", partitionsToResume)}]");
            }
            pausedPartitions = null;
        }

        public void CompleteAdding()
        {
            eventsQueue.CompleteAdding();
        }

        public void WaitForTermination()
        {
            processorThread.Join();
            processor.Release(routingKey);
        }

        private void ProcessorThreadFunc()
        {
            try
            {
                while (!eventsQueue.IsCompleted)
                {
                    var messageBatch = DequeueMessageBatch();
                    if (messageBatch.Count > 0)
                        ProcessMessageBatch(messageBatch);
                }
            }
            catch (Exception e)
            {
                log.Fatal("Unhandled exception on processor thread", e);
                throw;
            }
        }

        private List<Message<Null, byte[]>> DequeueMessageBatch()
        {
            var sw = Stopwatch.StartNew();
            var messageBatch = new List<Message<Null, byte[]>>();
            var dequeueTimeout = maxDequeueTimeout;
            while (true)
            {
                if (eventsQueue.TryTake(out var message, dequeueTimeout))
                {
                    messageBatch.Add(message);
                    if (messageBatch.Count >= maxBatchSize)
                        break;
                }
                else
                {
                    if (stopSignal.IsCancellationRequested)
                    {
                        if (eventsQueue.IsCompleted)
                            break;
                        dequeueTimeout = TimeSpan.Zero;
                    }
                    else
                    {
                        var elapsed = sw.Elapsed;
                        if (elapsed > maxDequeueTimeout)
                            break;
                        dequeueTimeout = maxDequeueTimeout - elapsed;
                        if (dequeueTimeout < minDequeueTimeout)
                            dequeueTimeout = minDequeueTimeout;
                    }
                }
            }
            return messageBatch;
        }

        private void ProcessMessageBatch(List<Message<Null, byte[]>> messageBatch)
        {
            var airlockEvents = new List<AirlockEvent<byte[]>>();
            var offsetsToCommit = new Dictionary<TopicPartition, long>();
            foreach (var x in messageBatch)
            {
                airlockEvents.Add(
                    new AirlockEvent<byte[]>
                    {
                        RoutingKey = routingKey,
                        Timestamp = x.Timestamp.UtcDateTime,
                        Payload = x.Value,
                    });
                offsetsToCommit[x.TopicPartition] = x.Offset + 1;
            }
            DoProcessMessageBatch(airlockEvents);
            consumer.CommitAsync(offsetsToCommit.Select(x => new TopicPartitionOffset(x.Key, x.Value))).GetAwaiter().GetResult();
        }

        private void DoProcessMessageBatch(List<AirlockEvent<byte[]>> airlockEvents)
        {
            try
            {
                processor.Process(airlockEvents);
            }
            catch (Exception e)
            {
                log.Error($"Processor failed for routingKey: {routingKey}, processorType: {processor.GetType().Name}, processorId: {processor.ProcessorId}", e);
            }
        }
    }
}
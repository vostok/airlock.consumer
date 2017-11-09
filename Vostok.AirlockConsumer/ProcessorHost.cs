using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Confluent.Kafka;
using Vostok.Logging;
using Vostok.Metrics;
using Vostok.Metrics.Meters;

namespace Vostok.AirlockConsumer
{
    public class ProcessorHost : IDisposable
    {
        private readonly string routingKey;
        private readonly IAirlockEventProcessor processor;
        private readonly ILog log;
        private readonly Consumer<Null, byte[]> consumer;
        private readonly ProcessorHostSettings processorHostSettings;
        private readonly TimeSpan maxDequeueTimeout = TimeSpan.FromSeconds(1);
        private readonly TimeSpan minDequeueTimeout = TimeSpan.FromMilliseconds(100);
        private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        private readonly BlockingCollection<Message<Null, byte[]>> eventsQueue = new BlockingCollection<Message<Null, byte[]>>(new ConcurrentQueue<Message<Null, byte[]>>());
        private readonly Thread processorThread;
        private int[] pausedPartitions;
        private readonly ICounter messageEnqueuedCounter;
        private readonly ICounter messageProcessedCounter;
        private readonly IDisposable queueGauge;
        private readonly IDisposable pausedGauge;

        public ProcessorHost(string consumerGroupHostId, string routingKey, IAirlockEventProcessor processor, ILog log, Consumer<Null, byte[]> consumer, IMetricScope metricScope, TimeSpan flushMetricsInterval, ProcessorHostSettings processorHostSettings)
        {
            this.routingKey = routingKey;
            this.processor = processor;
            this.log = log;
            this.consumer = consumer;
            this.processorHostSettings = processorHostSettings;
            AssignedPartitions = new int[0];
            processorThread = new Thread(ProcessorThreadFunc)
            {
                IsBackground = true,
                Name = $"processor-{consumerGroupHostId}-{processor.ProcessorId}",
            };
            queueGauge = metricScope.Gauge(flushMetricsInterval, "queue_size", () => eventsQueue.Count);
            pausedGauge = metricScope.Gauge(flushMetricsInterval, "paused", () => pausedPartitions != null ? 1 : 0);
            messageEnqueuedCounter = metricScope.Counter(flushMetricsInterval, "message_enqueued");
            messageProcessedCounter = metricScope.Counter(flushMetricsInterval, "message_processed");
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
            messageEnqueuedCounter.Add();
            eventsQueue.Add(message, CancellationToken.None);
            if (eventsQueue.Count >= processorHostSettings.MaxProcessorQueueSize)
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

        public void Stop()
        {
            cancellationTokenSource.Cancel();
        }

        public void WaitForTermination()
        {
            processorThread.Join();
            processor.Release(routingKey);
            cancellationTokenSource.Dispose();
        }

        private void ProcessorThreadFunc()
        {
            try
            {
                while (true)
                {
                    var messageBatch = DequeueMessageBatch();
                    if (cancellationTokenSource.IsCancellationRequested)
                        break;
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
            while (!cancellationTokenSource.IsCancellationRequested)
            {
                if (TryDequeueSingleMessage(dequeueTimeout, out var message))
                {
                    messageBatch.Add(message);
                    if (messageBatch.Count >= processorHostSettings.MaxBatchSize)
                        break;
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
            return messageBatch;
        }

        private bool TryDequeueSingleMessage(TimeSpan dequeueTimeout, out Message<Null, byte[]> message)
        {
            message = null;
            try
            {
                return eventsQueue.TryTake(out message, (int) dequeueTimeout.TotalMilliseconds, cancellationTokenSource.Token);
            }
            catch (OperationCanceledException)
            {
                return false;
            }
        }

        private void ProcessMessageBatch(List<Message<Null, byte[]>> messageBatch)
        {
            var airlockEvents = new List<AirlockEvent<byte[]>>();
            var offsetsToCommit = new Dictionary<TopicPartition, long>();
            foreach (var x in messageBatch)
            {
                var airlockEvent = new AirlockEvent<byte[]>
                {
                    RoutingKey = routingKey,
                    Timestamp = x.Timestamp.UtcDateTime,
                    Payload = x.Value,
                };
                airlockEvents.Add(airlockEvent);
                offsetsToCommit[x.TopicPartition] = x.Offset + 1;
            }
            DoProcessMessageBatch(airlockEvents);
            messageProcessedCounter.Add(airlockEvents.Count);
            consumer.CommitAsync(offsetsToCommit.Select(x => new TopicPartitionOffset(x.Key, x.Value))).GetAwaiter().GetResult();
        }

        private void DoProcessMessageBatch(List<AirlockEvent<byte[]>> airlockEvents)
        {
            try
            {
                processor.Process(airlockEvents, messageProcessedCounter);
                log.Info($"batch was processed, size={airlockEvents.Count}");
            }
            catch (Exception e)
            {
                log.Error($"Processor failed for routingKey: {routingKey}, processorType: {processor.GetType().Name}, processorId: {processor.ProcessorId}", e);
            }
        }

        public void Dispose()
        {
            (messageEnqueuedCounter as IDisposable)?.Dispose();
            (messageProcessedCounter as IDisposable)?.Dispose();
            queueGauge.Dispose();
            pausedGauge.Dispose();
        }
}
}
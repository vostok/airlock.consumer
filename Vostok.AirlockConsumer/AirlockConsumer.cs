using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Vostok.Airlock;
using Vostok.Logging;

namespace Vostok.AirlockConsumer
{
    public class AirlockConsumer<T> : IDisposable
    {
        private readonly int batchSize;
        private readonly IMessageProcessor<T> messageProcessor;
        private readonly ILog log;
        private readonly Consumer<byte[], T> consumer;
        private readonly CancellationTokenSource cancellationTokenSource;
        private readonly List<AirlockEvent<T>> events = new List<AirlockEvent<T>>();
        private readonly Dictionary<TopicPartition, long> lastOffsets = new Dictionary<TopicPartition, long>();
        private Task polltask;

        protected AirlockConsumer(Dictionary<string, object> settings, string[] topics, IAirlockDeserializer<T> deserializer, IMessageProcessor<T> messageProcessor, ILog log)
        {
            batchSize = int.Parse((string)settings["airlock.consumer.batch.size"]);
            this.messageProcessor = messageProcessor;
            this.log = log;
            events.Capacity = batchSize;
            var consumerDeserializer = new ConsumerDeserializer<T>(deserializer);
            consumer = new Consumer<byte[], T>(Clean(settings), new ByteArrayDeserializer(), consumerDeserializer);
            consumer.OnMessage += (s, e) => OnMessage(e);
            consumer.OnError += (s, e) => { log.Error(e.Reason); };
            consumer.OnConsumeError += (s, e) => { log.Error(e.Error.ToString()); };
            consumer.OnPartitionsAssigned += (s, e) =>
            {
                log.Info($"Assigned partitions: [{string.Join(", ", e)}], member id: {consumer.MemberId}");
                consumer.Assign(e.Select(x => new TopicPartitionOffset(x, Offset.Beginning)));
            };
            consumer.OnPartitionsRevoked += (_, e) =>
            {
                log.Info($"Revoked partitions: [{string.Join(", ", e)}]");
                consumer.Unassign();
            };
            cancellationTokenSource = new CancellationTokenSource();

            consumer.Subscribe(topics);
        }

        private static Dictionary<string, object> Clean(Dictionary<string, object> settings)
        {
            foreach (var key in settings.Keys.ToArray())
            {
                if (key.StartsWith("airlock.consumer"))
                    settings.Remove(key);
            }
            return settings;
        }

        public void Start()
        {
            var cancellationToken = cancellationTokenSource.Token;
            if (polltask != null)
                throw new InvalidOperationException("already started");
            polltask = new Task(() =>
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    bool wasError = false;
                    lock (this)
                    {
                        if (events.Count > 0)
                            wasError = !ProcessEvents();
                    }
                    if (!wasError)
                        consumer.Poll(100);
                }
            });
            polltask.Start();
        }

        public void Stop()
        {
            cancellationTokenSource.Cancel();
            polltask.Wait();
            polltask = null;
            lock (this)
            {
                ProcessEvents();
            }
        }

        private void OnMessage(Message<byte[], T> message)
        {
            var topic = message.Topic;
            log.Debug($"Got message, topic: '{topic}', ts: '{message.Timestamp.UtcDateTime:O}'");
            var dashPos = topic.LastIndexOf("-", StringComparison.Ordinal);
            string project;
            if (dashPos > 0)
                project = topic.Substring(0, dashPos);
            else
            {
                log.Error("Invalid topic name: '" + topic + "'");
                return;
            }
            var topicPartition = message.TopicPartition;
            lock (this)
            {
                if (lastOffsets.TryGetValue(message.TopicPartition, out var offset))
                {
                    lastOffsets[topicPartition] = Math.Max(offset, message.Offset.Value);
                }
                else
                {
                    lastOffsets[topicPartition] = message.Offset.Value;
                }
                events.Add(new AirlockEvent<T>
                {
                    Payload = message.Value,
                    Timestamp = message.Timestamp.UtcDateTime,
                    RoutingKey = project
                });
                if (events.Count >= batchSize)
                {
                    ProcessEvents();
                }
            }
        }

        private bool ProcessEvents()
        {
            try
            {
                if (events.Count == 0)
                    return true;
                messageProcessor.Process(events);
                try
                {
                    var committedOffsets = consumer.CommitAsync(lastOffsets.Select(x => new TopicPartitionOffset(x.Key, x.Value+1))).Result;
                    if (committedOffsets.Error != null && committedOffsets.Error.HasError)
                    {
                        log.Error("Commit error: " + committedOffsets.Error);
                    }
                }
                catch (Exception e)
                {
                    log.Error(e, "Commit error");
                }
                lastOffsets.Clear();
                return true;
            }
            catch (Exception ex)
            {
                log.Error(ex, $"Error during processing {events.Count} events");
                return false;
            }
            finally
            {
                events.Clear();
                events.Capacity = batchSize;
            }
        }

        public void Dispose()
        {
            cancellationTokenSource.Cancel();
            consumer?.Dispose();
        }
    }
}
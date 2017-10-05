using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Vostok.Tracing;

namespace Vostok.AirlockConsumer.Tracing
{
    public class TracingAirlockEventProcessor : IAirlockEventProcessor<Span>, IDisposable
    {
        private readonly ICassandraDataScheme dataScheme;
        private readonly ICassandraRetryExecutionStrategy retryExecutionStrategy;
        private readonly BoundedBlockingQueue<Span> spanQueue;
        private readonly CancellationTokenSource cancellationTokenSource;
        private readonly Task processingTask;

        public TracingAirlockEventProcessor(ICassandraDataScheme dataScheme, ICassandraRetryExecutionStrategy retryExecutionStrategy, int maxCassandraTasks)
        {
            this.dataScheme = dataScheme;
            this.retryExecutionStrategy = retryExecutionStrategy;
            spanQueue = new BoundedBlockingQueue<Span>(maxCassandraTasks);
            cancellationTokenSource = new CancellationTokenSource();
            var token = cancellationTokenSource.Token;
            processingTask = new Task(
                () =>
                {
                    Parallel.ForEach(
                        spanQueue.GetConsumingEnumerable(),
                        new ParallelOptions {MaxDegreeOfParallelism = maxCassandraTasks, CancellationToken = token },
                        ProcessSpan);
                }, token, TaskCreationOptions.LongRunning);
            processingTask.Start();
        }

        public Task ProcessAsync(List<AirlockEvent<Span>> events)
        {
            events.ForEach(x => spanQueue.Add(x.Payload));
            return Task.CompletedTask;
        }

        private void ProcessSpan(Span span)
        {
            retryExecutionStrategy.ExecuteAsync(dataScheme.GetInsertStatement(span)).Wait();
        }

        public void Dispose()
        {
            spanQueue.CompleteAdding();
            processingTask.Wait();
            cancellationTokenSource.Cancel();
            spanQueue.Dispose();
            cancellationTokenSource?.Dispose();
        }
    }
}
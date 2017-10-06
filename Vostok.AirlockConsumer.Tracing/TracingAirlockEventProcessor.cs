using System.Collections.Generic;
using System.Threading.Tasks;
using Vostok.Tracing;

namespace Vostok.AirlockConsumer.Tracing
{
    public class TracingAirlockEventProcessor : SimpleAirlockEventProcessorBase<Span>
    {
        private readonly ICassandraDataScheme dataScheme;
        private readonly ICassandraRetryExecutionStrategy retryExecutionStrategy;
        private readonly int maxCassandraTasks;

        public TracingAirlockEventProcessor(ICassandraDataScheme dataScheme, ICassandraRetryExecutionStrategy retryExecutionStrategy, int maxCassandraTasks)
        {
            this.dataScheme = dataScheme;
            this.retryExecutionStrategy = retryExecutionStrategy;
            this.maxCassandraTasks = maxCassandraTasks;
        }

        public override void Process(List<AirlockEvent<Span>> events)
        {
            Parallel.ForEach(events, new ParallelOptions {MaxDegreeOfParallelism = maxCassandraTasks}, ProcessEvent);
        }

        private void ProcessEvent(AirlockEvent<Span> @event)
        {
            retryExecutionStrategy.ExecuteAsync(dataScheme.GetInsertStatement(@event.Payload)).Wait();
        }
    }
}
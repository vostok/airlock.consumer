using System.Collections.Generic;
using System.Threading.Tasks;
using Vostok.Contrails.Client;
using Vostok.Tracing;

namespace Vostok.AirlockConsumer.Tracing
{
    public class TracingAirlockEventProcessor : SimpleAirlockEventProcessorBase<Span>
    {
        private readonly ThrowableLazy<IContrailsClient> contrailsClient;
        private readonly int maxCassandraTasks;

        public TracingAirlockEventProcessor(ThrowableLazy<IContrailsClient> contrailsClient, int maxCassandraTasks) // ICassandraDataScheme dataScheme, ICassandraRetryExecutionStrategy retryExecutionStrategy
        {
            this.contrailsClient = contrailsClient;
            this.maxCassandraTasks = maxCassandraTasks;
        }

        public sealed override void Process(List<AirlockEvent<Span>> events)
        {
            Parallel.ForEach(events, new ParallelOptions {MaxDegreeOfParallelism = maxCassandraTasks}, ProcessEvent);
        }

        private void ProcessEvent(AirlockEvent<Span> @event)
        {
            contrailsClient.Value.AddSpan(@event.Payload).Wait();
        }
    }
}
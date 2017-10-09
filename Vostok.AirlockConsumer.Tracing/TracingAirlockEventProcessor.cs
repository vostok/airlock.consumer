using System.Collections.Generic;
using System.Threading.Tasks;
using Vostok.Contrails.Client;
using Vostok.Tracing;

namespace Vostok.AirlockConsumer.Tracing
{
    public class TracingAirlockEventProcessor : SimpleAirlockEventProcessorBase<Span>
    {
        private readonly IContrailsClient contrailsClient;
        private readonly int maxCassandraTasks;

        public TracingAirlockEventProcessor(IContrailsClient contrailsClient, int maxCassandraTasks) // ICassandraDataScheme dataScheme, ICassandraRetryExecutionStrategy retryExecutionStrategy
        {
            this.contrailsClient = contrailsClient;
            this.maxCassandraTasks = maxCassandraTasks;
        }

        public override void Process(List<AirlockEvent<Span>> events)
        {
            Parallel.ForEach(events, new ParallelOptions {MaxDegreeOfParallelism = maxCassandraTasks}, ProcessEvent);
        }

        private void ProcessEvent(AirlockEvent<Span> @event)
        {
            contrailsClient.AddSpan(@event.Payload).Wait();
        }
    }
}
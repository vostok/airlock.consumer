using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Vostok.Contrails.Client;
using Vostok.Logging;
using Vostok.Metrics.Meters;
using Vostok.Tracing;

namespace Vostok.AirlockConsumer.Tracing
{
    public class TracingAirlockEventProcessor : SimpleAirlockEventProcessorBase<Span>
    {
        private readonly IContrailsClient contrailsClient;
        private readonly int maxCassandraTasks;
        private readonly ILog log;

        public TracingAirlockEventProcessor(IContrailsClient contrailsClient, ILog log, int maxCassandraTasks)
        {
            this.contrailsClient = contrailsClient;
            this.maxCassandraTasks = maxCassandraTasks;
            this.log = log;
        }

        public sealed override void Process(List<AirlockEvent<Span>> events, ICounter messageProcessedCounter)
        {
            Parallel.ForEach(events, new ParallelOptions {MaxDegreeOfParallelism = maxCassandraTasks}, @event => ProcessEvent(@event, messageProcessedCounter));
        }

        private void ProcessEvent(AirlockEvent<Span> @event, ICounter messageProcessedCounter)
        {
            try
            {
                contrailsClient.AddSpan(@event.Payload).GetAwaiter().GetResult();
                messageProcessedCounter.Add();
            }
            catch (Exception e)
            {
                log.Error(e);
            }
        }
    }
}
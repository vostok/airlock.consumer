using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Vostok.Contrails.Client;
using Vostok.Logging;
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

        public sealed override void Process(List<AirlockEvent<Span>> events, ProcessorMetrics processorMetrics)
        {
            Parallel.ForEach(events, new ParallelOptions {MaxDegreeOfParallelism = maxCassandraTasks}, @event => ProcessEvent(@event, processorMetrics));
        }

        private void ProcessEvent(AirlockEvent<Span> @event, ProcessorMetrics processorMetrics)
        {
            try
            {
                contrailsClient.AddSpan(@event.Payload).GetAwaiter().GetResult();
                processorMetrics.EventProcessedCounter.Add();
            }
            catch (Exception e)
            {
                processorMetrics.EventFailedCounter.Add();
                processorMetrics.SendingErrorCounter.Add();
                log.Error(e);
            }
        }
    }
}
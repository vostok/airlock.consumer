using System.Collections.Generic;
using System.Threading.Tasks;
using Vostok.Tracing;

namespace Vostok.AirlockConsumer.Tracing
{
    public class TracingAirlockEventProcessor : IAirlockEventProcessor<Span>
    {
        public Task ProcessAsync(List<AirlockEvent<Span>> events)
        {
            throw new System.NotImplementedException();
        }
    }
}
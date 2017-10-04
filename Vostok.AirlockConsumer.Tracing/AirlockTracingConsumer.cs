using System.Collections.Generic;
using Vostok.Airlock;
using Vostok.Logging;
using Vostok.Tracing;

namespace Vostok.AirlockConsumer.Tracing
{
    public class AirlockTracingConsumer : AirlockConsumer<Span>
    {
        public AirlockTracingConsumer(Dictionary<string, object> settings)
            //int eventType, int batchSize, IAirlockDeserializer<Span> deserializer, IMessageProcessor<Span> messageProcessor, ILog log, string settingsFileName = null)
            : base(settings, new[] { "vostok:staging|trace" }, new SpanAirlockSerializer(), new AirlockTracingProcessor(), AirlockConsumerTracingEntryPoint.Log.ForContext<AirlockLogEventConsumer>())
        {
        }
    }

    public class AirlockTracingProcessor : IMessageProcessor<Span>
    {
        public void Process(List<AirlockEvent<Span>> events)
        {
            throw new System.NotImplementedException();
        }
    }
}
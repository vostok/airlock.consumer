using System.Collections.Generic;
using Vostok.Airlock;
using Vostok.Logging;
using Vostok.Tracing;

namespace Vostok.AirlockConsumer.Tracing
{
    public class AirlockTracingConsumer : AirlockConsumer<Span>
    {
        public AirlockTracingConsumer(Dictionary<string, object> settings, CassandraDataScheme dataScheme)
            : base(settings, new[] { "vostok:staging|trace" }, new SpanAirlockSerializer(), new AirlockTracingProcessor(dataScheme), AirlockConsumerTracingEntryPoint.Log.ForContext<AirlockTracingConsumer>())
        {
        }
    }
}
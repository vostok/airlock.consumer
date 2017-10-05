using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Cassandra;
using Newtonsoft.Json;
using Vostok.Tracing;

namespace Vostok.AirlockConsumer.Tracing
{
    public class TracingAirlockEventProcessor : IAirlockEventProcessor<Span>
    {
        private readonly CassandraDataScheme dataScheme;
        private readonly CassandraRetryExecutionStrategy retryExecutionStrategy;

        public TracingAirlockEventProcessor(CassandraDataScheme dataScheme, CassandraRetryExecutionStrategy retryExecutionStrategy)
        {
            this.dataScheme = dataScheme;
            this.retryExecutionStrategy = retryExecutionStrategy;
        }

        public Task ProcessAsync(List<AirlockEvent<Span>> events)
        {
            return Task.WhenAll(events.Select(airlockEvent => retryExecutionStrategy.ExecuteAsync(dataScheme.GetInsertStatement(airlockEvent.Payload))));
        }
    }
}
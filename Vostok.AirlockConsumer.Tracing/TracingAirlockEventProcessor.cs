using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Cassandra;
using Newtonsoft.Json;
using Vostok.Tracing;

namespace Vostok.AirlockConsumer.Tracing
{
    public class TracingAirlockEventProcessor : IAirlockEventProcessor<Span>
    {
        private readonly CassandraDataScheme dataScheme;

        public TracingAirlockEventProcessor(CassandraDataScheme dataScheme)
        {
            this.dataScheme = dataScheme;
        }

        public Task ProcessAsync(List<AirlockEvent<Span>> events)
        {
            var batchStatement = new BatchStatement();
            batchStatement.SetBatchType(BatchType.Unlogged);
            batchStatement.SetIdempotence(true);
            foreach (var airlockEvent in events)
            {
                var insertStatement = dataScheme.GetInsertStatement(airlockEvent.Payload);
                batchStatement.Add(insertStatement);
            }
            dataScheme.Session.Execute(batchStatement);
            return Task.CompletedTask;
        }
    }
}
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
        private readonly JsonSerializer jsonSerializer;

        public TracingAirlockEventProcessor(CassandraDataScheme dataScheme)
        {
            this.dataScheme = dataScheme;
            jsonSerializer = new JsonSerializer();
        }

        public Task ProcessAsync(List<AirlockEvent<Span>> events)
        {
            var batchStatement = new BatchStatement();
            batchStatement.SetBatchType(BatchType.Unlogged);
            batchStatement.SetIdempotence(true);
            foreach (var airlockEvent in events)
            {
                var span = airlockEvent.Payload;
                var stringWriter = new StringWriter();
                jsonSerializer.Serialize(stringWriter, span.Annotations);
                var spanInfo = new SpanInfo
                {
                    BeginTimestamp = span.BeginTimestamp,
                    EndTimestamp = span.EndTimestamp,
                    Annotations = stringWriter.ToString(),
                    ParentSpanId = span.ParentSpanId,
                    SpanId = span.SpanId,
                    TraceId = span.TraceId
                };

                var insertStatement = dataScheme.GetInsertStatement(spanInfo);
                batchStatement.Add(insertStatement);
            }
            dataScheme.Session.Execute(batchStatement);
            return Task.CompletedTask;
        }
    }
}
using System.Collections.Generic;
using System.IO;
using Cassandra;
using Newtonsoft.Json;
using Vostok.Tracing;

namespace Vostok.AirlockConsumer.Tracing
{
    public class AirlockTracingProcessor : IMessageProcessor<Span>
    {
        private readonly CassandraDataScheme dataScheme;
        private readonly JsonSerializer jsonSerializer;

        public AirlockTracingProcessor(CassandraDataScheme dataScheme)
        {
            this.dataScheme = dataScheme;
            jsonSerializer = new JsonSerializer();
        }

        public void Process(List<AirlockEvent<Span>> events)
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
        }
    }
}
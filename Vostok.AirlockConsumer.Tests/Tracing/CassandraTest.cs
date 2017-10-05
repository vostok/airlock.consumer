using System;
using System.Collections.Generic;
using Cassandra;
using NUnit.Framework;
using Vostok.AirlockConsumer.Tracing;
using Vostok.Tracing;

namespace Vostok.AirlockConsumer.Tests.Tracing
{
    public class CassandraTest
    {
        public static Lazy<ISession> Session = new Lazy<ISession>(
            () =>
            {
                var sessionKeeper = new CassandraSessionKeeper(new[] { "localhost:9042" }, "airlock");
                return sessionKeeper.Session;
            });

        public static CassandraDataScheme DataScheme
        {
            get
            {
                var dataScheme = new CassandraDataScheme(Session.Value, "spans");
                dataScheme.CreateTableIfNotExists();
                return dataScheme;
            }
        }

        [Test, Explicit("Manual")]
        public void InsertData()
        {
            var dataScheme = DataScheme;
            var insertStatement = dataScheme.GetInsertStatement(
                new Span
                {
                    Annotations = new Dictionary<string, string> { ["key"] = "value" },
                    TraceId = Guid.NewGuid(),
                    SpanId = Guid.NewGuid(),
                    BeginTimestamp = DateTimeOffset.UtcNow,
                    EndTimestamp = DateTimeOffset.UtcNow.AddMinutes(10),
                    ParentSpanId = Guid.NewGuid()
                });
            Session.Value.Execute(insertStatement);
        }
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using Cassandra;
using NUnit.Framework;

namespace Vostok.AirlockConsumer.Tracing.Tests
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
                new SpanInfo
                {
                    Annotations = "{ \"key\" : \"value\"}",
                    TraceId = Guid.NewGuid(),
                    SpanId = Guid.NewGuid(),
                    BeginTimestamp = DateTimeOffset.UtcNow,
                    EndTimestamp = DateTimeOffset.UtcNow.AddMinutes(10),
                });
            Session.Value.Execute(insertStatement);
        }
    }
}
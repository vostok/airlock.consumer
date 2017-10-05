﻿using System;
using System.Threading;
using Cassandra;
using Cassandra.Mapping;
using Cassandra.Data.Linq;

namespace Vostok.AirlockConsumer.Tracing
{
    public class CassandraDataScheme 
    {
        private readonly Lazy<PreparedStatement> preparedInsert;

        public CassandraDataScheme(
            ISession session,
            string tableName)
        {
            Session = session;
            TableName = tableName;
            preparedInsert = new Lazy<PreparedStatement>(PrepareInsert, LazyThreadSafetyMode.ExecutionAndPublication);
        }

        public ISession Session { get; }
        public string TableName { get; }
        public Table<SpanInfo> Table => new Table<SpanInfo>(Session, MappingConfiguration.Global, TableName);

        public Statement GetInsertStatement(SpanInfo span)
        {
            return preparedInsert.Value.Bind(
                span.EndTimestamp,
                span.Annotations,
                span.TraceIdPrefix,
                span.TraceId,
                span.SpanId,
                span.BeginTimestamp);
        }

        public void CreateTableIfNotExists()
        {
            var createQuery = string.Format(CreateTableQueryTemplate, TableName);
            Session.Execute(createQuery);
        }

        private PreparedStatement PrepareInsert()
        {
            var preparedQuery = string.Format(PreparedInsertTemplate, TableName);
            var statement = Session.Prepare(preparedQuery);
            statement.SetIdempotence(true);
            return statement;
        }

        #region Cassandra schemes and mapping

        private const string PreparedInsertTemplate =
            @"update {0} 
              set 
                end_timestamp = ?,
                annotations = ? 
              where 
                trace_id_prefix = ? and
                trace_id = ? and
                span_id = ? and
                begin_timestamp = ?";

        private const string CreateTableQueryTemplate =
            @"CREATE TABLE IF NOT EXISTS {0} (
    trace_id_prefix text,
    begin_timestamp timestamp,
    end_timestamp timestamp,
    trace_id uuid,
    span_id uuid,
    annotations text,
    PRIMARY KEY (trace_id_prefix, begin_timestamp, trace_id, span_id))
    WITH CLUSTERING ORDER BY (begin_timestamp ASC, trace_id ASC, span_id ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {{'keys': 'ALL', 'rows_per_partition': 'NONE'}} 
    AND comment = '' 
    AND crc_check_chance = 1.0
    AND compaction = {{
        'class': 'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy',
        'compaction_window_unit': 'MINUTES',
        'compaction_window_size': '150'}}
    AND compression = {{
        'chunk_length_in_kb': '64',
        'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}}
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 259200
    AND gc_grace_seconds = 10800
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';";


        #endregion
    }
}
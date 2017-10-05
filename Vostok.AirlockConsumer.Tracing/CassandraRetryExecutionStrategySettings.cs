using System;
using System.Collections.Generic;
using Vostok.Commons.Extensions.UnitConvertions;

namespace Vostok.AirlockConsumer.Tracing
{
    public class CassandraRetryExecutionStrategySettings
    {
        public int CassandraSaveRetryMaxAttempts { get; } = 3;
        public TimeSpan CassandraSaveRetryMinDelay { get; } = 1.Seconds();
        public TimeSpan CassandraSaveRetryMaxDelay { get; } = 3.Seconds();

        public CassandraRetryExecutionStrategySettings()
        {
            
        }

        public CassandraRetryExecutionStrategySettings(Dictionary<string, object> settings)
        {
            if (settings == null)
                return;
            if (settings["cassandra.save.retry.max.attempts"] != null)
            {
                CassandraSaveRetryMaxAttempts = int.Parse(settings["cassandra.save.retry.max.attempts"].ToString());
            }
            if (settings["cassandra.save.retry.min.delay"] != null)
            {
                CassandraSaveRetryMinDelay = TimeSpan.Parse(settings["cassandra.save.retry.min.delay"].ToString());
            }
            if (settings["cassandra.save.retry.max.delay"] != null)
            {
                CassandraSaveRetryMaxDelay = TimeSpan.Parse(settings["cassandra.save.retry.max.delay"].ToString());
            }
        }
    }
}
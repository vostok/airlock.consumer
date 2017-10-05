using System;
using Vostok.Commons.Extensions.UnitConvertions;

namespace Vostok.AirlockConsumer.Tracing
{
    public class CassandraRetryExecutionStrategySettings
    {
        public int CassandraSaveRetryMaxAttempts { get; } = 3;
        public TimeSpan CassandraSaveRetryMinDelay { get; } = 1.Seconds();
        public TimeSpan CassandraSaveRetryMaxDelay { get; } = 3.Seconds();
    }
}
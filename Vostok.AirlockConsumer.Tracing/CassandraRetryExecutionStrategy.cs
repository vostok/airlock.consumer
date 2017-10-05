using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cassandra;
using Vostok.Commons.Utilities;
using Vostok.Logging;

namespace Vostok.AirlockConsumer.Tracing
{
    internal class CassandraRetryExecutionStrategy 
    {
        private readonly ISession session;
        private readonly int cassandraSaveRetryMaxAttempts;
        private readonly TimeSpan cassandraSaveRetryMinDelay;
        private readonly TimeSpan cassandraSaveRetryMaxDelay;
        private readonly ILog log;
        private const double MinDelayMultiplier = 1.7;
        private const double MaxDelayMultiplier = 2.5;

        public CassandraRetryExecutionStrategy(Dictionary<string, object> settings, ILog log, ISession session)
        {
            this.session = session;
            cassandraSaveRetryMaxAttempts = int.Parse(settings["cassandra.save.retry.max.attempts"].ToString());
            cassandraSaveRetryMinDelay = TimeSpan.Parse(settings["cassandra.save.retry.min.delay"].ToString());
            cassandraSaveRetryMaxDelay = TimeSpan.Parse(settings["cassandra.save.retry.max.delay"].ToString());
            this.log = log.ForContext(this);
        }

        public async Task ExecuteAsync(Statement statement)
        {
            var maxAttemptsCount = 1 + Math.Max(0, cassandraSaveRetryMaxAttempts);
            var delay = cassandraSaveRetryMinDelay;

            for (var attempt = 1; attempt < maxAttemptsCount + 1; attempt++)
            {
                if (attempt != 1)
                {
                    log.Warn($"Will try to save again in {delay:g}");
                    await Task.Delay(delay);
                    delay = IncreaseDelay(delay);
                }

                try
                {
                    await session.ExecuteAsync(statement);
                    if (attempt != 1)
                    {
                        log.Info($"Save succeed after {attempt} attempts");
                    }
                    return;
                }
                catch (Exception ex)
                {
                    if (ex is WriteTimeoutException || ex is NoHostAvailableException || ex is WriteFailureException)
                        continue;
                    log.Error($"Save failed at {attempt} attempt. Will drop this span insert.", ex);
                    return;
                }
            }

            log.Error($"Exceeded max retry attempts limit (tried {maxAttemptsCount} times). Will drop this span insert.");
        }

        private TimeSpan IncreaseDelay(TimeSpan delay)
        {
            var multiplier = MinDelayMultiplier
                             + ThreadSafeRandom.NextDouble() * (MaxDelayMultiplier - MinDelayMultiplier);
            var increasedDelay = delay.Multiply(multiplier);
            return TimeSpanExtensions.Min(
                TimeSpanExtensions.Max(cassandraSaveRetryMinDelay, increasedDelay),
                cassandraSaveRetryMaxDelay);
        }
    }
}
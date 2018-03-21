using System;

namespace Vostok.Airlock.Consumer.Sentry
{
    public class SentryProcessorSettings
    {
        public int MaxTasks { get; set; }
        public TimeSpan ThrottlingPeriod { get; set; }
        public int ThrottlingThreshold { get; set; }
    }
}
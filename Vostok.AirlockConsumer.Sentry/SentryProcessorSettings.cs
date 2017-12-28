using System;
using Vostok.Commons.Extensions.UnitConvertions;

namespace Vostok.AirlockConsumer.Sentry
{
    public class SentryProcessorSettings
    {
        public int MaxTasks { get; set; } = 100;
        public TimeSpan ThrottlingPeriod { get; set; } = 1.Minutes();
        public int ThrottlingThreshold { get; set; } = 100;
    }
}
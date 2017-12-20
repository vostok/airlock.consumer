using FluentAssertions;
using NSubstitute;
using Vostok.Metrics;

namespace Vostok.AirlockConsumer.UnitTests.Sentry
{
    public class TestProcessorMetrics : ProcessorMetrics
    {
        public TestProcessorMetrics(): base(Substitute.For<IMetricScope>(), 1.Minutes())
        {
            
        }
    }
}
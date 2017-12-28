using FluentAssertions;
using NSubstitute;
using Vostok.Metrics;

namespace Vostok.AirlockConsumer.UnitTests
{
    public class TestProcessorMetrics : ProcessorMetrics
    {
        public TestProcessorMetrics(): base(Substitute.For<IMetricScope>(), 1.Minutes())
        {
            
        }
    }
}
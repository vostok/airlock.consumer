using FluentAssertions;
using NSubstitute;
using Vstk.Metrics;

namespace Vstk.AirlockConsumer.UnitTests
{
    public class TestProcessorMetrics : ProcessorMetrics
    {
        public TestProcessorMetrics(): base(Substitute.For<IMetricScope>(), 1.Minutes())
        {
            
        }
    }
}
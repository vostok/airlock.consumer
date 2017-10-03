using NSubstitute;
using Vostok.Logging;
using Xunit.Abstractions;

namespace Vostok.AirlockConsumer.Tests
{
    public static class TestSetup
    {
        public static ILog GetLogMock(ITestOutputHelper outputHelper)
        {
            var logMock = Substitute.For<ILog>();
            logMock
                .When(l => l.Log(Arg.Any<LogEvent>()))
                .Do(info => new TestOutputLog(outputHelper).Log(info.Arg<LogEvent>()));
            return logMock;
        }
    }
}
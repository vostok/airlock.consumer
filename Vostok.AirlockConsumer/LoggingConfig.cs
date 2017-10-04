using Serilog;
using Vostok.Logging;
using Vostok.Logging.Serilog;

namespace Vostok.AirlockConsumer
{
    public static class Logging
    {
        public static ILog Configure(string logFilePattern)
        {
            var logger = new LoggerConfiguration()
                .WriteTo.Console()
                .WriteTo.RollingFile(logFilePattern)
                .MinimumLevel.Debug()
                .CreateLogger();
            return new SerilogLog(logger).WithFlowContext();
        }
    }
}
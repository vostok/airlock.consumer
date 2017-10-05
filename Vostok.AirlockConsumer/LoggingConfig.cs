using Serilog;
using Vostok.Logging;
using Vostok.Logging.Serilog;
using Vostok.Logging.Serilog.Enrichers;

namespace Vostok.AirlockConsumer
{
    public static class Logging
    {
        public static ILog Configure(string logFilePattern)
        {
            var logger = new LoggerConfiguration()
                .Enrich.With<ThreadEnricher>()
                .WriteTo.Console(outputTemplate: "{Timestamp:HH:mm:ss.fff} {Level:u3} [{Thread}] {Message:l}{NewLine}{Exception}")
                .WriteTo.RollingFile(logFilePattern)
                .MinimumLevel.Debug()
                .CreateLogger();
            return new SerilogLog(logger).WithFlowContext();
        }
    }
}
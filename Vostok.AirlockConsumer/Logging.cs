using Serilog;
using Serilog.Events;
using Vostok.Logging;
using Vostok.Logging.Serilog;
using Vostok.Logging.Serilog.Enrichers;

namespace Vostok.AirlockConsumer
{
    public static class Logging
    {
        public static ILog Configure(string pathFormat = "./log/actions-{Date}.log")
        {
            var logger = new LoggerConfiguration()
                .Enrich.With<ThreadEnricher>()
                .MinimumLevel.Debug()
                .WriteTo.Console(outputTemplate: "{Timestamp:HH:mm:ss.fff} {Level:u3} [{Thread}] {Message:l}{NewLine}{Exception}", restrictedToMinimumLevel: LogEventLevel.Information)
                .WriteTo.RollingFile(pathFormat, outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff} {Level:u3} [{Thread}] {SourceContext} {Message:l}{NewLine}{Exception}")
                .CreateLogger();
            return new SerilogLog(logger).WithFlowContext();
        }
    }
}

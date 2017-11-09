using Serilog;
using Serilog.Events;
using Vostok.Logging;
using Vostok.Logging.Serilog;
using Vostok.Logging.Serilog.Enrichers;

namespace Vostok.AirlockConsumer
{
    public static class Logging
    {
        public static ILog Configure()
        {
            var logger = new LoggerConfiguration()
                .Enrich.With<ThreadEnricher>()
                .MinimumLevel.Debug()
                .WriteTo.Console(outputTemplate: "{Timestamp:HH:mm:ss.fff} {Level:u3} [{Thread}] {Message:l}{NewLine}{Exception}", restrictedToMinimumLevel: LogEventLevel.Information)
                .WriteTo.RollingFile("./log/actions-{Date}.log", outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff} {Level:u3} [{Thread}] {SourceContext} {Message:l}{NewLine}{Exception}")
                .CreateLogger();
            return new SerilogLog(logger).WithFlowContext();
        }
        public static ILog ConfigureAirlockClient()
        {
            var logger = new LoggerConfiguration()
                .Enrich.With<ThreadEnricher>()
                .MinimumLevel.Debug()
                .WriteTo.RollingFile("./log/airlock-{Date}.log", outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff} {Level:u3} [{Thread}] {SourceContext} {Message:l}{NewLine}{Exception}")
                .CreateLogger();
            return new SerilogLog(logger).WithFlowContext();
        }
    }
}
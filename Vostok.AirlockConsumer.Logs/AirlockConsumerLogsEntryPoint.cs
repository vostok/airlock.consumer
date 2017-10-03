using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using Serilog;
using Vostok.Logging;
using Vostok.Logging.Serilog;

namespace Vostok.AirlockConsumer.Logs
{
    public static class AirlockConsumerLogsEntryPoint
    {
        public static ILog Log;

        public static void Main(string[] args)
        {
            var settings = Util.ReadYamlSettings<Dictionary<string, object>>(GetSettingsFileName(args));
            settings["client.id"] = Dns.GetHostName();

            var logger = new LoggerConfiguration()
                .WriteTo.Console()
                .WriteTo.RollingFile((string)settings["airlock.consumer.log.file.pattern"])
                .MinimumLevel.Debug()
                .CreateLogger();
            Log = new SerilogLog(logger);
            var consumer = new AirlockLogEventConsumer(settings);
            consumer.Start();
            Console.ReadLine();
            consumer.Stop();
        }

        private static string GetSettingsFileName(string[] args)
        {
            return args.Any() ? args[0] : "default-settings.yaml";
        }
    }
}

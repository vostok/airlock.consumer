namespace Vostok.AirlockConsumer.Tracing
{
    public class TracingAirlockConsumerEntryPoint
    {
        public static ILog Log;

        private static void Main(string[] args)
        {
            var settings = Util.ReadYamlSettings<Dictionary<string, object>>(GetSettingsFileName(args));
            settings["client.id"] = Dns.GetHostName();

            var logger = new LoggerConfiguration()
                .WriteTo.Console()
                .WriteTo.RollingFile((string)settings["airlock.consumer.log.file.pattern"])
                .MinimumLevel.Debug()
                .CreateLogger();
            Log = new SerilogLog(logger);
            try
            {
                var sessionKeeper = new CassandraSessionKeeper(((List<object>) settings["cassandra.endpoints"]).Cast<string>(), (string)settings["cassandra.keyspace"]);
                var dataScheme = new CassandraDataScheme(sessionKeeper.Session, (string)settings["cassandra.spans.tablename"]);
                dataScheme.CreateTableIfNotExists();
                var consumer = new AirlockTracingConsumer(settings, dataScheme);
                consumer.Start();
                Console.ReadLine();
                consumer.Stop();

            }
            catch (Exception e)
            {
                Log.Error(e);
                throw;
            }
        }

        private static string GetSettingsFileName(string[] args)
        {
            return args.Any() ? args[0] : "default-settings.yaml";
        }

    }
}
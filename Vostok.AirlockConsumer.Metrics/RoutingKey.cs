namespace Vostok.AirlockConsumer.Metrics
{
    public class RoutingKey
    {
        private const string defaultValue = "unknown";

        public static RoutingKey Parse(string routingKey)
        {
            Airlock.RoutingKey.TryParse(routingKey, out var project, out var env, out var service, out _);
            return new RoutingKey(project ?? defaultValue, env ?? defaultValue, service ?? defaultValue);
        }

        private RoutingKey(string project, string environment, string serviceName)
        {
            Project = project;
            Environment = environment;
            ServiceName = serviceName;
        }

        public string Project { get; }
        public string Environment { get; }
        public string ServiceName { get; }
    }
}
namespace Vostok.AirlockConsumer.Metrics
{
    internal class RoutingKey
    {
        private const string defaultValue = "unknown";

        private RoutingKey(string project, string environment, string serviceName)
        {
            Project = project;
            Environment = environment;
            ServiceName = serviceName;
        }

        public string Project { get; }
        public string Environment { get; }
        public string ServiceName { get; }

        public static RoutingKey Parse(string routingKey)
        {
            Airlock.RoutingKey.TryParse(routingKey, out var project, out var env, out var service, out _);
            return new RoutingKey(project ?? defaultValue, env ?? defaultValue, service ?? defaultValue);
        }
    }
}
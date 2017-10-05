namespace Vostok.AirlockConsumer.Metrics
{
    internal class RoutingKey
    {
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
            const char separator = '.';
            var split = routingKey.Split(separator);

            return new RoutingKey(GetValue(split, 0), GetValue(split, 1), GetValue(split, 2));
        }

        private static string GetValue(string[] values, int index)
        {
            const string defaultValue = "Unknown";
            if (values.Length <= index)
            {
                return defaultValue;
            }

            var result = values[index];
            return string.IsNullOrEmpty(result) ? defaultValue : result;
        }
    }
}
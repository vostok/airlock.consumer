namespace Vostok.AirlockConsumer.Metrics
{
    internal class RoutingKey
    {
        public string Project { get; set; }
        public string Environment { get; set; }
        public string ServiceName { get; set; }

        public static RoutingKey Parse(string routingKey)
        {
            const char separator = ':';
            var split = routingKey.Split(separator);

            return new RoutingKey
            {
                Project = GetValue(split, 0),
                Environment = GetValue(split, 1),
                ServiceName = GetValue(split, 2)
            };
        }

        private static string GetValue(string[] values, int index)
        {
            const string defaultValue = "Any";
            if (values.Length <= index)
            {
                return defaultValue;
            }

            var result = values[index];
            return string.IsNullOrEmpty(result) ? defaultValue : result;
        }
    }
}
using System.Linq;

namespace Vostok.AirlockConsumer.TracesToEvents
{
    internal static class TraceEventsRoutingKeyBuilder
    {
        public static string Build(string tracingRoutingKey, string serviceName)
        {
            const string suffix = "trace_events";
            const string separator = ":";
            var split = tracingRoutingKey.Split(separator);
            var prefix = string.Join(separator, split.Take(split.Length - 1));

            return prefix + separator + serviceName + separator + suffix;
        }
    }
}
using System.Collections.Generic;

namespace Vostok.AirlockConsumer.Metrics
{
    internal interface IGraphiteNameBuilder
    {
        string BuildPrefix(string routingKey, IEnumerable<KeyValuePair<string, string>> tags);
        string BuildName(string prefix, string suffix);
    }
}
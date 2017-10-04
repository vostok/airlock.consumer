using System.Collections.Generic;

namespace Vostok.AirlockConsumer.FinalMetrics
{
    internal interface IGraphiteNameBuilder
    {
        string Build(string routingKey, IEnumerable<KeyValuePair<string, string>> tags);
        string Build(string prefix, string name);
    }
}
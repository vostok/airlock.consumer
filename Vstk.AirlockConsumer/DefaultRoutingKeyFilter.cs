using Vstk.Airlock;

namespace Vstk.AirlockConsumer
{
    public class DefaultRoutingKeyFilter : IRoutingKeyFilter
    {
        private readonly string lastRoutingKeySuffix;

        public DefaultRoutingKeyFilter(string lastRoutingKeySuffix)
        {
            this.lastRoutingKeySuffix = lastRoutingKeySuffix;
        }

        public bool Matches(string routingKey) => RoutingKey.LastSuffixMatches(routingKey, lastRoutingKeySuffix);
    }
}
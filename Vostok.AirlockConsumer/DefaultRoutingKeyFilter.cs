namespace Vostok.AirlockConsumer
{
    public class DefaultRoutingKeyFilter : IRoutingKeyFilter
    {
        private readonly string routingKeySuffix;

        public DefaultRoutingKeyFilter(string routingKeySuffix)
        {
            this.routingKeySuffix = routingKeySuffix;
        }

        public bool Matches(string routingKey) => routingKey.EndsWith(routingKeySuffix);
    }
}
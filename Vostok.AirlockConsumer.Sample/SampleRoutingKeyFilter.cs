using System.Linq;

namespace Vostok.AirlockConsumer.Sample
{
    public class SampleRoutingKeyFilter : IRoutingKeyFilter
    {
        private readonly string[] routingKeys;

        public SampleRoutingKeyFilter(string[] routingKeys)
        {
            this.routingKeys = routingKeys;
        }

        public bool Matches(string routingKey) => routingKeys.Contains(routingKey);
    }
}
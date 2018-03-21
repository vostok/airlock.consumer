namespace Vostok.AirlockConsumer
{
    public interface IRoutingKeyFilter
    {
        bool Matches(string routingKey);
    }
}
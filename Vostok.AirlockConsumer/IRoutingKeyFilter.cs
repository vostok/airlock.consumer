namespace Vstk.AirlockConsumer
{
    public interface IRoutingKeyFilter
    {
        bool Matches(string routingKey);
    }
}
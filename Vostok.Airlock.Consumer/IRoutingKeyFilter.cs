namespace Vostok.Airlock.Consumer
{
    public interface IRoutingKeyFilter
    {
        bool Matches(string routingKey);
    }
}
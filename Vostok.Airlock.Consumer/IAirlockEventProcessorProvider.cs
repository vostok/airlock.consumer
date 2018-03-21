namespace Vostok.Airlock.Consumer
{
    public interface IAirlockEventProcessorProvider
    {
        IAirlockEventProcessor GetProcessor(string routingKey);
    }
}
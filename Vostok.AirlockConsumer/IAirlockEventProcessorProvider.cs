namespace Vostok.AirlockConsumer
{
    public interface IAirlockEventProcessorProvider
    {
        IAirlockEventProcessor GetProcessor(string routingKey);
    }
}
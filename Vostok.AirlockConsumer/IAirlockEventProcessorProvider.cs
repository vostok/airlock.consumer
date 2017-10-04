namespace Vostok.AirlockConsumer
{
    public interface IAirlockEventProcessorProvider
    {
        IAirlockEventProcessor TryGetProcessor(string routingKey);
    }
}
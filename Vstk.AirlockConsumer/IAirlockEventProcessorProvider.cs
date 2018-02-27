namespace Vstk.AirlockConsumer
{
    public interface IAirlockEventProcessorProvider
    {
        IAirlockEventProcessor GetProcessor(string routingKey);
    }
}
namespace Vostok.AirlockConsumer
{
    public interface IProcessorProvider
    {
        IProcessor TryGetProcessor(string routingKey);
    }
}
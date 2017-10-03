namespace Vostok.AirlockConsumer.MetricsAggregator
{
    internal interface IMeterFactory
    {
        IMeter Create(string valueName);
    }
}
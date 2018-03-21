namespace Vostok.AirlockConsumer
{
    public class ProcessorHostSettings
    {
        public int MaxBatchSize { get; set; } = 100_000;
        public int MaxProcessorQueueSize { get; set; } = 1000_000;
    }
}
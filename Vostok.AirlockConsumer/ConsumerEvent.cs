namespace Vostok.AirlockConsumer
{
    public class ConsumerEvent<T>
    {
        public T Event { get; set; }
        public long Timestamp { get; set; }
        public string Project { get; set; }
    }
}
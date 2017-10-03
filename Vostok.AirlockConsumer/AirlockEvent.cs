using System;

namespace Vostok.AirlockConsumer
{
    public class AirlockEvent<T>
    {
        public DateTime Timestamp { get; set; }
        public string RoutingKey { get; set; }
        public T Payload { get; set; }
    }
}
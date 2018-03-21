using System;

namespace Vstk.AirlockConsumer
{
    public class AirlockEvent<T>
    {
        public string RoutingKey { get; set; }
        public DateTimeOffset Timestamp { get; set; }
        public T Payload { get; set; }
    }
}
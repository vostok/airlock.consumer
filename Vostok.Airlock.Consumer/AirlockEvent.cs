using System;

namespace Vostok.Airlock.Consumer
{
    public class AirlockEvent<T>
    {
        public string RoutingKey { get; set; }
        public DateTimeOffset Timestamp { get; set; }
        public T Payload { get; set; }
    }
}
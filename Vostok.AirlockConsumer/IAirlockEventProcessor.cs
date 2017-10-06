using System.Collections.Generic;

namespace Vostok.AirlockConsumer
{
    public interface IAirlockEventProcessor
    {
        string ProcessorId { get; }

        void Process(List<AirlockEvent<byte[]>> events);
    }

    public interface IAirlockEventProcessor<T>
    {
        void Process(List<AirlockEvent<T>> events);
    }
}
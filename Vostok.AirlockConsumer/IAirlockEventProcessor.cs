using System.Collections.Generic;
using System.Threading.Tasks;

namespace Vostok.AirlockConsumer
{
    public interface IAirlockEventProcessor
    {
        string ProcessorId { get; }

        Task ProcessAsync(List<AirlockEvent<byte[]>> events);
    }

    public interface IAirlockEventProcessor<T>
    {
        Task ProcessAsync(List<AirlockEvent<T>> events);
    }
}
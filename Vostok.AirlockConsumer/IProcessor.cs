using System.Collections.Generic;
using System.Threading.Tasks;

namespace Vostok.AirlockConsumer
{
    public interface IProcessor
    {
        string ProcessorId { get; }

        Task ProcessAsync(List<AirlockEvent<byte[]>> events);
    }
}
using System.Threading;
using Serilog.Core;
using Serilog.Events;

namespace Vostok.AirlockConsumer
{
    public class ManagedThreadIdEnricher : ILogEventEnricher
    {
        public void Enrich(LogEvent logEvent, ILogEventPropertyFactory propertyFactory)
        {
            logEvent.AddPropertyIfAbsent(propertyFactory.CreateProperty("Thread", Thread.CurrentThread.Name ?? Thread.CurrentThread.ManagedThreadId.ToString(), false));
        }
    }
}
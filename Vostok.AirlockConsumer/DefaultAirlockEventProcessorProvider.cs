using System;
using System.Collections.Concurrent;
using Vostok.Airlock;

namespace Vostok.AirlockConsumer
{
    public class DefaultAirlockEventProcessorProvider<T, TDeserializer> : IAirlockEventProcessorProvider
        where TDeserializer : IAirlockDeserializer<T>, new()
    {
        private readonly Func<string, IAirlockEventProcessor<T>> createProcessorForProject;
        private readonly IAirlockDeserializer<T> airlockDeserializer = new TDeserializer();
        private readonly ConcurrentDictionary<string, DefaultAirlockEventProcessor<T>> processorsByProject = new ConcurrentDictionary<string, DefaultAirlockEventProcessor<T>>();

        public DefaultAirlockEventProcessorProvider(Func<string, IAirlockEventProcessor<T>> createProcessorForProject)
        {
            this.createProcessorForProject = createProcessorForProject;
        }

        public IAirlockEventProcessor GetProcessor(string routingKey)
        {
            RoutingKey.Parse(routingKey, out var project, out _, out _, out _);
            return processorsByProject.GetOrAdd(project, x => new DefaultAirlockEventProcessor<T>(airlockDeserializer, createProcessorForProject(project)));
        }
    }
}
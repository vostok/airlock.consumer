using System;
using System.Collections.Generic;

namespace Vostok.Airlock.Consumer
{
    public class DefaultAirlockEventProcessorProvider<T, TDeserializer> : IAirlockEventProcessorProvider
        where TDeserializer : IAirlockDeserializer<T>, new()
    {
        private readonly Func<string, IAirlockEventProcessor<T>> createProcessorForProject;
        private readonly IAirlockDeserializer<T> airlockDeserializer = new TDeserializer();
        private readonly Dictionary<string, DefaultAirlockEventProcessor<T>> processorsByProject = new Dictionary<string, DefaultAirlockEventProcessor<T>>();

        public DefaultAirlockEventProcessorProvider(Func<string, IAirlockEventProcessor<T>> createProcessorForProject)
        {
            this.createProcessorForProject = createProcessorForProject;
        }

        public IAirlockEventProcessor GetProcessor(string routingKey)
        {
            RoutingKey.Parse(routingKey, out var project, out _, out _, out _);
            if (!processorsByProject.TryGetValue(project, out var processor))
            {
                processor = new DefaultAirlockEventProcessor<T>(airlockDeserializer, createProcessorForProject(project));
                processorsByProject.Add(project, processor);
            }
            return processor;
        }
    }
}
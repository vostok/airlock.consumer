using System;
using System.Collections.Generic;
using Vostok.Airlock;

namespace Vostok.AirlockConsumer.Sentry
{
    public class SentryAirlockProcessorProvider<T, TDeserializer> : IAirlockEventProcessorProvider
        where TDeserializer : IAirlockDeserializer<T>, new()
    {
        private readonly Func<string, string, IAirlockEventProcessor<T>> createProcessorForProjectAndEnv;
        private readonly IAirlockDeserializer<T> airlockDeserializer = new TDeserializer();
        private readonly Dictionary<string, DefaultAirlockEventProcessor<T>> processorsByProjectAndEnv = new Dictionary<string, DefaultAirlockEventProcessor<T>>();

        public SentryAirlockProcessorProvider(Func<string, string, IAirlockEventProcessor<T>> createProcessorForProjectAndEnv)
        {
            this.createProcessorForProjectAndEnv = createProcessorForProjectAndEnv;
        }

        public IAirlockEventProcessor GetProcessor(string routingKey)
        {
            RoutingKey.Parse(routingKey, out var project, out var environment, out _, out _);
            if (!processorsByProjectAndEnv.TryGetValue($"{project}_{environment}", out var processor))
            {
                processor = new DefaultAirlockEventProcessor<T>(airlockDeserializer, createProcessorForProjectAndEnv(project, environment));
                processorsByProjectAndEnv.Add(project, processor);
            }
            return processor;
        }
    }
}
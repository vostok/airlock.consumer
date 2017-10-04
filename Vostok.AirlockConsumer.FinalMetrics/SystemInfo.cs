using System.Collections.Generic;
using System.Text;

namespace Vostok.AirlockConsumer.FinalMetrics
{
    internal class SystemInfo
    {
        public string Project { get; set; }
        public string Environment { get; set; }
        public string ServiceName { get; set; }

        public static SystemInfo Parse(string routingKey)
        {
            const char separator = ':';
            var project = new StringBuilder();
            var environment = new StringBuilder();
            var serviceName = new StringBuilder();
            var queueBuilders = new Queue<StringBuilder>(3);
            queueBuilders.Enqueue(project);
            queueBuilders.Enqueue(environment);
            queueBuilders.Enqueue(serviceName);

            var currentBuilder = queueBuilders.Dequeue();

            foreach (var c in routingKey)
            {
                if (c.Equals(separator))
                {
                    if (queueBuilders.Count == 0)
                    {
                        break;
                    }
                    currentBuilder = queueBuilders.Dequeue();
                    continue;
                }

                currentBuilder.Append(c);
            }

            return new SystemInfo
            {
                Project = project.Length > 0 ? project.ToString() : "Any",
                Environment = environment.Length > 0 ? environment.ToString() : "Any",
                ServiceName = serviceName.Length > 0 ? serviceName.ToString() : "Any",
            };
        }
    }
}
using System.Collections.Generic;
using SharpRaven;

namespace Vostok.AirlockConsumer.Sentry
{
    public class SentryClientProvider
    {
        private readonly SentryApiClient apiClient;
        private readonly Dictionary<string, RavenClient> projectToClient = new Dictionary<string, RavenClient>();
        private static readonly object locker = new object();

        public SentryClientProvider(SentryApiClient apiClient)
        {
            this.apiClient = apiClient;
        }

        public RavenClient GetOrCreateClient(string project)
        {
            if (projectToClient.TryGetValue(project, out var client))
                return client;
            lock (locker)
            {
                if (projectToClient.TryGetValue(project, out client))
                    return client;
                var dsn = apiClient.GetOrCreateProjectAndDsn(project);
                client = new RavenClient(dsn);
                projectToClient.Add(project, client);
                return client;
            }
        }
    }
}
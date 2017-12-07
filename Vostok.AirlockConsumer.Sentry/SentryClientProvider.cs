using System.Collections.Generic;
using System.Linq;
using System.Net;
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
                SentryApiClient.SentryTeam team;
                try
                {
                    team = apiClient.GetTeam(project);
                }
                catch (HttpListenerException e) when (e.ErrorCode == 404)
                {
                    team = null;
                }
                if (team == null)
                {
                    apiClient.CreateTeam(project);
                    apiClient.CreateProject(project, project);
                }
                else
                {
                    if (!team.Projects.Contains(project))
                        apiClient.CreateProject(project, project);
                }
                var dsn = apiClient.GetProjectDsn(project) ?? apiClient.CreateProjectDsn(project);
                client = new RavenClient(dsn);
                projectToClient.Add(project, client);
                return client;
            }
        }
    }
}
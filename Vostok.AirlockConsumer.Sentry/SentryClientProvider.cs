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

        public RavenClient GetOrCreateClient(string project, string env)
        {
            var sentryProject = $"{project}_{env}";
            var sentryTeam = project;
            if (projectToClient.TryGetValue(sentryProject, out var client))
                return client;
            lock (locker)
            {
                if (projectToClient.TryGetValue(sentryProject, out client))
                    return client;
                SentryApiClient.SentryTeam team;
                try
                {
                    team = apiClient.GetTeam(sentryTeam);
                }
                catch (HttpListenerException e) when (e.ErrorCode == 404)
                {
                    team = null;
                }
                if (team == null)
                {
                    apiClient.CreateTeam(sentryTeam);
                    apiClient.CreateProject(sentryTeam, sentryProject);
                }
                else
                {
                    if (!team.Projects.Contains(sentryProject))
                        apiClient.CreateProject(sentryTeam, sentryProject);
                }
                var dsn = apiClient.GetProjectDsn(sentryProject) ?? apiClient.CreateProjectDsn(sentryProject);
                client = new RavenClient(dsn);
                projectToClient.Add(sentryProject, client);
                return client;
            }
        }
    }
}
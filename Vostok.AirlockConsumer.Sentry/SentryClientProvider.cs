using System.Linq;
using System.Net;
using SharpRaven;

namespace Vostok.AirlockConsumer.Sentry
{
    public class SentryClientProvider
    {
        private readonly SentryApiClient apiClient;

        public SentryClientProvider(SentryApiClient apiClient)
        {
            this.apiClient = apiClient;
        }

        public RavenClient CreateClient(string project, string env)
        {
            var sentryProject = $"{project}_{env}";
            var sentryTeam = project;
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
            var client = new RavenClient(dsn);
            return client;
        }
    }
}
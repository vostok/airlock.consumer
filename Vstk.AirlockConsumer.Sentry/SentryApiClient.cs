using System;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using Newtonsoft.Json.Linq;
using Vstk.Logging;
using Vstk.RetriableCall;

namespace Vstk.AirlockConsumer.Sentry
{
    public class SentryApiClient
    {
        private readonly ILog log;
        private const string apiPrefix = "/api/0";
        private readonly string organization;
        private readonly HttpClient httpClient;
        private readonly RetriableCallStrategy callStrategy = new RetriableCallStrategy();

        public SentryApiClient(SentryApiClientSettings settings, ILog log)
        {
            organization = settings.Organization;
            this.log = log;

            httpClient = new HttpClient
            {
                BaseAddress = new Uri(settings.Url)
            };
            httpClient.DefaultRequestHeaders.Add("Authorization", "Bearer " + settings.Token);
            httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        }

        public SentryTeam GetTeam(string team)
        {
            var msg = Get($"/teams/{organization}/{team}/");
            dynamic teamObj = JToken.Parse(msg);
            var projectsJson = Get($"/teams/{organization}/{team}/projects/");
            var projArr = JArray.Parse(projectsJson);
            return new SentryTeam
            {
                Name = teamObj.slug,
                Projects = projArr.Cast<dynamic>().Select(proj => (string) proj.slug).ToArray()
            };
        }

        public void CreateTeam(string team)
        {
            Post($"/organizations/{organization}/teams/", new {name = team, slug = team});
        }

        public void CreateProject(string team, string project)
        {
            Post($"/teams/{organization}/{team}/projects/", new {name = project, slug = project});
        }

        public string GetProjectDsn(string project)
        {
            var msg = Get($"/projects/{organization}/{project}/keys/");
            var dsnArr = JArray.Parse(msg);
            var activeDsn = dsnArr.Cast<dynamic>().FirstOrDefault(x => (bool) x.isActive);
            return activeDsn?.dsn.secret;
        }

        public string CreateProjectDsn(string project)
        {
            var newDsn = Post($"/projects/{organization}/{project}/keys/", new {name = "default"});
            return newDsn.dsn.secret;
        }

        private string Get(string uri)
        {
            try
            {
                return callStrategy.Call(
                    () =>
                    {
                        var httpResponseMessage = httpClient.GetAsync($"{apiPrefix}{uri}").GetAwaiter().GetResult();
                        if (!httpResponseMessage.IsSuccessStatusCode)
                            throw new HttpListenerException((int)httpResponseMessage.StatusCode, httpResponseMessage.ReasonPhrase);
                        return httpResponseMessage.Content.ReadAsStringAsync().GetAwaiter().GetResult();
                    }, IsExceptionRetriable, log);

            }
            catch (Exception ex)
            {
                throw new Exception("Could not get data by URL " + uri, ex);
            }
        }

        private static readonly HttpStatusCode[] retriableHttpStatusCodes =
        {
            HttpStatusCode.BadGateway,
            HttpStatusCode.GatewayTimeout,
            HttpStatusCode.RequestTimeout,
            HttpStatusCode.ServiceUnavailable,
            HttpStatusCode.TemporaryRedirect,
        };

        private bool IsExceptionRetriable(Exception ex)
        {
            if (ex is HttpListenerException httpListenerEx)
            {
                return retriableHttpStatusCodes.Contains((HttpStatusCode) httpListenerEx.ErrorCode);
            }
            return ex is HttpRequestException;
        }

        private dynamic Post(string uri, object body)
        {
            try
            {
                var content = new StringContent(body.ToJson());
                content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
                return callStrategy.Call(
                    () =>
                    {
                        var httpResponseMessage = httpClient.PostAsync($"{apiPrefix}{uri}", content).GetAwaiter().GetResult();
                        if (!httpResponseMessage.IsSuccessStatusCode)
                            throw new HttpListenerException((int)httpResponseMessage.StatusCode, httpResponseMessage.ReasonPhrase);
                        var httpResponseBody = httpResponseMessage.Content.ReadAsStringAsync().GetAwaiter().GetResult();
                        return JToken.Parse(httpResponseBody);
                    }, IsExceptionRetriable, log);

            }
            catch (Exception e)
            {
                throw new Exception("Could not post data to URL " + uri, e);
            }
        }
    }
}
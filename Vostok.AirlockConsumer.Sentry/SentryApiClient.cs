using System;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using Newtonsoft.Json.Linq;

namespace Vostok.AirlockConsumer.Sentry
{
    public class SentryApiClient
    {
        private const string apiPrefix = "/api/0";
        private const string defaultOrgName = "sentry";

        private readonly string organization;
        private readonly HttpClient httpClient;

        public SentryApiClient(string hostUrl, string token, string organization = null)
        {
            this.organization = organization ?? defaultOrgName;
            httpClient = new HttpClient
            {
                BaseAddress = new Uri(hostUrl)
            };
            httpClient.DefaultRequestHeaders.Add("Authorization", "Bearer " + token);
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
            var httpResponseMessage = httpClient.GetAsync($"{apiPrefix}{uri}").GetAwaiter().GetResult();
            if (!httpResponseMessage.IsSuccessStatusCode)
                throw new HttpListenerException((int) httpResponseMessage.StatusCode, httpResponseMessage.ReasonPhrase);
            return httpResponseMessage.Content.ReadAsStringAsync().GetAwaiter().GetResult();
        }

        private dynamic Post(string uri, object body)
        {
            var content = new StringContent(body.ToJson());
            content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
            var httpResponseMessage = httpClient.PostAsync($"{apiPrefix}{uri}", content).GetAwaiter().GetResult();
            if (!httpResponseMessage.IsSuccessStatusCode)
                throw new HttpListenerException((int) httpResponseMessage.StatusCode, httpResponseMessage.ReasonPhrase);
            var httpResponseBody = httpResponseMessage.Content.ReadAsStringAsync().GetAwaiter().GetResult();
            return JToken.Parse(httpResponseBody);
        }

        public class SentryTeam
        {
            public string Name { get; set; }
            public string[] Projects { get; set; }
        }
    }
}
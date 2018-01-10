using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using SharpRaven;
using Vostok.Airlock;
using Vostok.Airlock.Logging;
using Vostok.Logging;

namespace Vostok.AirlockConsumer.Sentry
{
    public class SentryAirlockProcessorProvider : IAirlockEventProcessorProvider
    {
        private readonly SentryApiClient sentryApiClient;
        private readonly ILog log;
        private readonly SentryProcessorSettings processorSettings;
        private readonly LogEventDataSerializer airlockDeserializer = new LogEventDataSerializer();
        private readonly Dictionary<string, DefaultAirlockEventProcessor<LogEventData>> processorsByProjectAndEnv = new Dictionary<string, DefaultAirlockEventProcessor<LogEventData>>();

        public SentryAirlockProcessorProvider(SentryApiClient sentryApiClient, ILog log, SentryProcessorSettings processorSettings)
        {
            this.sentryApiClient = sentryApiClient;
            this.log = log;
            this.processorSettings = processorSettings;
        }

        public IAirlockEventProcessor GetProcessor(string routingKey)
        {
            RoutingKey.Parse(routingKey, out var project, out var environment, out _, out _);
            var projEnv = $"{project}_{environment}";
            if (!processorsByProjectAndEnv.TryGetValue(projEnv, out var processor))
            {
                var ravenClient = CreateRavenClient(project, environment);
                var sentryProjectId = ravenClient.CurrentDsn.ProjectID;
                var sentryPacketSender = new SentryPacketSender(ravenClient, log);
                var sentryAirlockProcessor = new SentryAirlockProcessor(sentryProjectId, processorSettings, log, sentryPacketSender);
                processor = new DefaultAirlockEventProcessor<LogEventData>(airlockDeserializer, sentryAirlockProcessor);
                processorsByProjectAndEnv.Add(projEnv, processor);
            }
            return processor;
        }

        private RavenClient CreateRavenClient(string project, string environment)
        {
            var sentryProject = $"{project}_{environment}";
            var sentryTeam = project;
            SentryTeam team;
            try
            {
                team = sentryApiClient.GetTeam(sentryTeam);
            }
            catch (Exception e) //when (e.ErrorCode == 404)
            {
                log.Error(e);
                team = null;
            }
            if (team == null)
            {
                sentryApiClient.CreateTeam(sentryTeam);
                sentryApiClient.CreateProject(sentryTeam, sentryProject);
            }
            else
            {
                if (!team.Projects.Contains(sentryProject))
                    sentryApiClient.CreateProject(sentryTeam, sentryProject);
            }
            var dsn = sentryApiClient.GetProjectDsn(sentryProject) ?? sentryApiClient.CreateProjectDsn(sentryProject);
            return new RavenClient(dsn);
        }
    }
}
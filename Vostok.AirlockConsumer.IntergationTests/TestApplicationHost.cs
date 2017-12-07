using System.Collections.Generic;
using Vostok.Logging;

namespace Vostok.AirlockConsumer.IntergationTests
{
    public class TestApplicationHost<TConsumerApp>
        where TConsumerApp : ConsumerApplication, new()
    {
        private readonly ILog log;
        private ConsumerGroupHost consumerGroupHost;

        public TestApplicationHost(ILog log)
        {
            this.log = log;
        }
        public void Run()
        {
            var consumerApplication = new TConsumerApp();
            var environmentVariables = new Dictionary<string, string>
            {
                ["AIRLOCK_ELASTICSEARCH_ENDPOINTS"] = "http://localhost:9200",
                ["AIRLOCK_GATE_ENDPOINTS"] = "http://localhost:6306",
                ["AIRLOCK_KAFKA_BOOTSTRAP_ENDPOINTS"] = "localhost:9092",
                ["AIRLOCK_CASSANDRA_ENDPOINTS"] = "localhost:9042",
            };
            consumerGroupHost = consumerApplication.Initialize(log, environmentVariables);
            log.Info($"Consumer application is initialized: {typeof(TConsumerApp).Name}");
            consumerGroupHost.Start();
        }

        public void Stop()
        {
            consumerGroupHost.Stop();
            log.Info($"Consumer application is stopped: {typeof(TConsumerApp).Name}");
        }
    }
}
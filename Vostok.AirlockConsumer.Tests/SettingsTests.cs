using System.Collections.Generic;
using Xunit;

namespace Vostok.AirlockConsumer.Tests
{
    public class SettingsTests
    {
        [Fact]
        public void ReadKafkaSettings()
        {
            var settings = Util.ReadYamlSettings<Dictionary<string, object>>("kafka.yaml");
            Assert.True(settings.ContainsKey("fetch.wait.max.ms"));
            Assert.Equal("1000", settings["fetch.wait.max.ms"]);
        }
    }
}
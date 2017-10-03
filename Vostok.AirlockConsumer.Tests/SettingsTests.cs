using System.Collections.Generic;
using FluentAssertions;
using NUnit.Framework;

namespace Vostok.AirlockConsumer.Tests
{
    public class SettingsTests
    {
        [Test]
        public void ReadKafkaSettings()
        {
            var settings = Util.ReadYamlSettings<Dictionary<string, object>>("kafka.yaml");
            settings.Should().ContainKey("fetch.wait.max.ms");
            settings["fetch.wait.max.ms"].Should().Be("1000");
        }
    }
}
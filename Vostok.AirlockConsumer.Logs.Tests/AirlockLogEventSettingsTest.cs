using FluentAssertions;
using NUnit.Framework;

namespace Vostok.AirlockConsumer.Logs.Tests
{
    public class AirlockLogEventSettingsTest
    {
        [Test]
        public void ReadLogEventSettings()
        {
            var settings = Util.ReadYamlSettings<AirlockLogEventSettings>("logConsumer.yaml");
            settings.BatchSize.Should().Be(1000);
        }
    }
}
using Xunit;

namespace Vostok.AirlockConsumer.Logs.Tests
{
    public class AirlockLogEventSettingsTest
    {
        [Fact]
        public void ReadLogEventSettings()
        {
            var settings = Util.ReadYamlSettings<AirlockLogEventSettings>("logConsumer.yaml");
            Assert.Equal(1000, settings.BatchSize);
        }
    }
}
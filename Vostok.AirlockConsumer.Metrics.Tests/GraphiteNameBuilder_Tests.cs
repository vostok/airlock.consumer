using System.Linq;
using FluentAssertions;
using NUnit.Framework;

namespace Vostok.AirlockConsumer.Metrics.Tests
{
    public class GraphiteNameBuilder_Tests
    {
        private GraphiteNameBuilder graphiteNameBuilder;

        [SetUp]
        public void SetUp()
        {
            graphiteNameBuilder = new GraphiteNameBuilder();
        }

        [TestCase("projectName.environmentName.serviceName", "projectName:environmentName:serviceName")]
        [TestCase("projectName.environmentName.serviceName", "projectName:environmentName:serviceName:other")]
        [TestCase("project_name.environment_name.service_name", "project.name:environment.name:service.name")]
        [TestCase("projectName.environmentName.Any", "projectName:environmentName")]
        [TestCase("projectName.Any.Any", "projectName")]
        [TestCase("Any.Any.Any", "")]
        [TestCase("Any.environmentName.Any", ":environmentName")]
        [TestCase("Any.environmentName.Any", ":environmentName:")]
        [TestCase("Any.Any.serviceName", "::serviceName")]
        [TestCase("__________._____.__________", "ИмяПроекта:Среда:ИмяСервиса")]
        [TestCase("Any.Any.Any.TypeName.hostname.Operation.status.Value3.Value1.Value2", "",
            "a2:Value1", "host:HOSTNAME", "a3:Value2", "type:TypeName", "operationName:Operation", "statusCode:status", "a1:Value3")]
        [TestCase("Any.Any.Any", "", "a:")]
        [TestCase("Any.Any.Any", "", ":v")]
        [TestCase("Any.Any.Any.________", "", "a:Значение")]
        [TestCase("Any.Any.Any.start_finish", "", "a:start.finish")]
        public void Build_should_build_name_by_routingKey_and_tags(string expecting, string routingKey, params string[] tagStrings)
        {
            var tags = tagStrings.Select(
                    x =>
                    {
                        var split = x.Split(":");
                        return new { Key = split[0], Value = split[1] };
                    }
                )
                .ToDictionary(x => x.Key, x => x.Value);

            var actual = graphiteNameBuilder.BuildPrefix(routingKey, tags);

            actual.Should().Be(expecting);
        }

        [TestCase("layer1.layer2.layer3.name", "name")]
        [TestCase("layer1.layer2.layer3.___", "Имя")]
        [TestCase("layer1.layer2.layer3.duration_p95", "duration.p95")]
        public void Build_should_build_name_by_prifix_and_name(string expecting, string name)
        {
            const string prefix = "layer1.layer2.layer3";

            var actual = graphiteNameBuilder.BuildName(prefix, name);

            actual.Should().Be(expecting);
        }
    }
}
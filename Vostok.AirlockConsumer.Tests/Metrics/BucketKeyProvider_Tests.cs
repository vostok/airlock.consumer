using System.Collections.Generic;
using System.Linq;
using FluentAssertions;
using NUnit.Framework;
using Vostok.AirlockConsumer.MetricsAggregator;

namespace Vostok.AirlockConsumer.Tests.Metrics
{
    public class BucketKeyProvider_Tests
    {
        [TestCase("", "a:1|b:2|c:3", "a:1|b:2|c:3")]
        [TestCase("a", "a:1|b:2|c:3", "a:1|b:2|c:3", "a:any|b:2|c:3")]
        [TestCase("b", "a:1|b:2|c:3", "a:1|b:2|c:3", "a:1|b:any|c:3")]
        [TestCase("c", "a:1|b:2|c:3", "a:1|b:2|c:3", "a:1|b:2|c:any")]
        [TestCase("a|b", "a:1|b:2|c:3", "a:1|b:2|c:3", "a:any|b:2|c:3", "a:1|b:any|c:3", "a:any|b:any|c:3")]
        [TestCase("b|c", "a:1|b:2|c:3", "a:1|b:2|c:3", "a:1|b:2|c:any", "a:1|b:any|c:3", "a:1|b:any|c:any")]
        [TestCase("a|b|c", "a:1|b:2|c:3", "a:1|b:2|c:3", "a:1|b:2|c:any", "a:1|b:any|c:3", "a:1|b:any|c:any", "a:any|b:2|c:3", "a:any|b:2|c:any", "a:any|b:any|c:3", "a:any|b:any|c:any")]
        public void GetBucketKeys_ReturnsValidCombinations(string splittableTags, string source, params string[] expected)
        {
            var provider = new BucketKeyProvider(splittableTags.Split("|"));
            var keys = provider.GetBucketKeys(Parse(source));
            keys.Select(Format).Should().BeEquivalentTo(expected);
        }

        [Test]
        public void GetBucketKeys_RealCase_ReturnsValidCombinations()
        {
            var provider = new BucketKeyProvider();
            var keys = provider.GetBucketKeys(Parse("host:vm1|operation:read|status:200|type:requests|gfv:100501"));
            keys.Select(Format)
                .Should()
                .BeEquivalentTo(
                    "host:vm1|operation:read|status:200|type:requests|gfv:100501",
                    "host:vm1|operation:read|status:any|type:requests|gfv:100501",
                    "host:vm1|operation:any|status:200|type:requests|gfv:100501",
                    "host:vm1|operation:any|status:any|type:requests|gfv:100501",
                    "host:any|operation:read|status:200|type:requests|gfv:100501",
                    "host:any|operation:read|status:any|type:requests|gfv:100501",
                    "host:any|operation:any|status:200|type:requests|gfv:100501",
                    "host:any|operation:any|status:any|type:requests|gfv:100501");
        }

        private static IReadOnlyDictionary<string, string> Parse(string source)
        {
            return source
                .Split("|")
                .Select(x => x.Split(":"))
                .ToDictionary(x => x[0], x => x[1]);
        }

        private static string Format(BucketKey key)
        {
            return string.Join("|", key.Tags.Select(x => $"{x.Key}:{x.Value}"));
        }
    }
}
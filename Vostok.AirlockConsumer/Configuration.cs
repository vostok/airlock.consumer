using System.Collections.Generic;
using System.IO;
using System.Linq;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

namespace Vostok.AirlockConsumer
{
    public static class Configuration
    {
        public static Dictionary<string, object> TryGetSettingsFromFile(string[] args)
        {
            return args.Any() ? ReadYaml<Dictionary<string, object>>(args[0]) : null;
        }

        public static T ReadYaml<T>(string fileName)
        {
            var input = new StringReader(File.ReadAllText(fileName));
            var deserializer = new DeserializerBuilder().WithNamingConvention(new PascalCaseNamingConvention()).Build();
            return deserializer.Deserialize<T>(input);
        }
    }
}
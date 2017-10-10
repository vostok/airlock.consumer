using JetBrains.Annotations;
using Newtonsoft.Json;

namespace Vostok.AirlockConsumer
{
    public static class JsonExtensions
    {
        [NotNull]
        public static string ToJson<T>([NotNull] this T obj, [NotNull] params JsonConverter[] converters)
        {
            return JsonConvert.SerializeObject(obj, converters);
        }

        [NotNull]
        public static string ToPrettyJson<T>([NotNull] this T obj, [NotNull] params JsonConverter[] converters)
        {
            return JsonConvert.SerializeObject(obj, Formatting.Indented, converters);
        }

        [NotNull]
        public static T FromJson<T>([NotNull] this string json)
        {
            return JsonConvert.DeserializeObject<T>(json);
        }
    }
}
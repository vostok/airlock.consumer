using System;

namespace Vostok.AirlockConsumer.Metrics
{
    public class TagInfo
    {
        public TagInfo(string name, int priority, Func<string, string> convertValue)
        {
            Name = name;
            Priority = priority;
            ConvertValue = convertValue;
        }

        public string Name { get; }
        public int Priority { get; }
        public Func<string, string> ConvertValue { get; }
    }
}
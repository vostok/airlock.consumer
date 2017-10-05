using System.Collections.Generic;
using Vostok.Airlock;
using Vostok.Commons.Binary;

namespace Vostok.AirlockConsumer.Tests
{
    public static class AirlockEventTypes
    {
        public const short Logging = 1;
        public const short Metrics = 2;
        public const short Tracing = 3;
    }

    public class EventRecord
    {
        public long Timestamp { get; set; }
        public byte[] Data { get; set; }
    }

    public class EventGroup
    {
        public short EventType { get; set; }
        public List<EventRecord> EventRecords { get; set; }
    }

    public class AirlockMessage
    {
        public short Version { get; } = 1;
        public List<EventGroup> EventGroups { get; set; }
    }

    public class AirlockMessageSerializer : IAirlockSerializer<AirlockMessage>
    {
        public void Serialize(AirlockMessage item, IAirlockSink sink)
        {
            var writer = sink.Writer;
            writer.Write(item.Version);
            writer.WriteCollection(item.EventGroups, WriteGroup);
        }

        private static void WriteGroup(IBinaryWriter writer, EventGroup g)
        {
            writer.Write(g.EventType);
            writer.WriteCollection(g.EventRecords, WriteRecord);
        }

        private static void WriteRecord(IBinaryWriter writer, EventRecord r)
        {
            writer.Write(r.Timestamp);
            writer.Write(r.Data);
        }
    }
}
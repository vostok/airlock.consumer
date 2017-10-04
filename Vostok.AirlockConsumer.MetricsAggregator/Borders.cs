using System;

namespace Vostok.AirlockConsumer.MetricsAggregator
{
    internal class Borders
    {
        public Borders(DateTimeOffset past, DateTimeOffset future)
        {
            Past = past;
            Future = future;
        }

        public DateTimeOffset Past { get; }
        public DateTimeOffset Future { get; }
    }
}
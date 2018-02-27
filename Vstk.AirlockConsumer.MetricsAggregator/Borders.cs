using System;

namespace Vstk.AirlockConsumer.MetricsAggregator
{
    public class Borders
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
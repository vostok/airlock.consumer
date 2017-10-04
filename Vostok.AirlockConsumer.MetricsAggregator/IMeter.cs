using System.Collections.Generic;

namespace Vostok.AirlockConsumer.MetricsAggregator
{
    internal class Meter
    {
        private double sum;

        public void Add(double value)
        {
            sum += value;
        }

        public IReadOnlyDictionary<string, double> Reset()
        {
            var result = new Dictionary<string, double>
            {
                { "sum", sum }
            };
            sum = 0;
            return result;
        }
    }
}
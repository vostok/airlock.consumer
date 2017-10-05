using System.Collections.Generic;
using Vostok.Metrics;

namespace Vostok.AirlockConsumer.MetricsAggregator
{
    internal class Meter
    {
        private double sum;

        public void Add(double value)
        {
            sum += value;
        }

        public IReadOnlyDictionary<string, double> GetValues()
        {
            var result = new Dictionary<string, double>
            {
                { "sum", sum }
            };
            return result;
        }
    }
}
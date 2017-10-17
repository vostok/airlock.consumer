using System.Collections.Generic;
using System.Threading;
using Vostok.Metrics.Meters.Histograms;

namespace Vostok.AirlockConsumer.MetricsAggregator
{
    public class Meter
    {
        private static readonly double[] percentiles = {0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999};

        private double sum;
        private readonly UniformHistogramReservoir histogram;

        public Meter()
        {
            sum = 0;
            histogram = new UniformHistogramReservoir();
        }

        public void Add(double value)
        {
            AddInterlocked(ref sum, value);
            //sum += value;
            histogram.Add(value);
        }

        public static double AddInterlocked(ref double location1, double value)
        {
            Thread.MemoryBarrier();
            var newCurrentValue = location1;
            while (true)
            {
                var currentValue = newCurrentValue;
                var newValue = currentValue + value;
                newCurrentValue = Interlocked.CompareExchange(ref location1, newValue, currentValue);
                // ReSharper disable once CompareOfFloatsByEqualityOperator
                if (newCurrentValue == currentValue)
                    return newValue;
            }
        }

        public IReadOnlyDictionary<string, double> GetValues()
        {
            var result = new Dictionary<string, double>
            {
                { "sum", sum }
            };

            var snapshot = histogram.GetSnapshot();
            if (snapshot.MeasurementsCount > 0)
            {
                RecordSnapshot(result, snapshot);
            }

            return result;
        }

        private static void RecordSnapshot(
            Dictionary<string, double> result,
            ReservoirHistogramSnapshot snapshot)
        {
            result.Add("mean", snapshot.Mean);
            result.Add("stddev", snapshot.StdDev);
            foreach (var percentile in percentiles)
            {
                var value = snapshot.GetUpperQuantile(percentile);
                result.Add($"upper{percentile*100:###}", value);
            }
        }
    }
}
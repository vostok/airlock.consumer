using System.Collections.Generic;
using Vostok.Commons.Synchronization;
using Vostok.Metrics.Meters.Histograms;

namespace Vostok.Airlock.Consumer.MetricsAggregator
{
    public class Meter
    {
        private static readonly double[] percentiles = {0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999};

        private readonly AtomicDouble sum = new AtomicDouble(0);
        private readonly UniformHistogramReservoir histogram;

        public Meter()
        {
            histogram = new UniformHistogramReservoir();
        }

        public void Add(double value)
        {
            sum.Add(value);
            histogram.Add(value);
        }

        public IReadOnlyDictionary<string, double> GetValues()
        {
            var result = new Dictionary<string, double>
            {
                {"sum", sum}
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
using System.Collections.Generic;

namespace Vostok.AirlockConsumer.MetricsAggregator
{
    internal interface IMeter
    {
        void Add(double value);
        IReadOnlyDictionary<string, double> Reset();
    }
}
using System;
using Vostok.Logging;

namespace Vostok.AirlockConsumer
{
    public class ThrowableLazy<T> where T : class
    {
        private T value;
        private readonly Func<T> func;
        private readonly ILog log;

        public ThrowableLazy(Func<T> func, ILog log)
        {
            this.func = func;
            this.log = log;
        }

        public T Value
        {
            get
            {
                try
                {
                    if (value != null)
                        return value;
                    lock (this)
                    {
                        if (value != null)
                            return value;
                        var temp = func();
                        System.Threading.Thread.MemoryBarrier();
                        value = temp;
                    }
                    return value;
                }
                catch (Exception e)
                {
                    log.Error(e);
                    throw;
                }
            }
        }
    }
}
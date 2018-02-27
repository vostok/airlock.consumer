using System;
using System.Diagnostics;
using System.Threading;
using JetBrains.Annotations;
using Vstk.Logging;

namespace Vstk.AirlockConsumer.IntergationTests
{
    public static class WaitHelper
    {
        public static void WaitSafe(Func<WaitAction> func, int timeOutSeconds = 120, bool throwException = true, string exceptionMessage = "Wait operation timed out.")
        {
            Wait(() =>
            {
                try
                {
                    return func();
                }
                catch (Exception ex)
                {
                    IntegrationTestsEnvironment.Log.Debug("Wait attempt failed", ex);
                    return WaitAction.ContinueWaiting;
                }
            }, timeOutSeconds, throwException, exceptionMessage);
        }

        public static void WaitSafe(Action action, int timeOutSeconds = 120, bool throwException = true, string exceptionMessage = "Wait operation timed out.")
        {
            Wait(() =>
            {
                try
                {
                    action();
                    return WaitAction.StopWaiting;
                }
                catch (Exception ex)
                {
                    IntegrationTestsEnvironment.Log.Debug("Wait attempt failed", ex);
                    return WaitAction.ContinueWaiting;
                }
            }, timeOutSeconds, throwException, exceptionMessage);
        }

        public static void Wait(Func<bool> func, int timeOutSeconds = 120, bool throwException = true, string exceptionMessage = "Wait operation timed out.")
        {
            Wait(() => func() ? WaitAction.StopWaiting : WaitAction.ContinueWaiting, timeOutSeconds, throwException, exceptionMessage);
        }

        public static void Wait(Func<WaitAction> func,
                                int timeOutSeconds = 120,
                                bool throwIfExpired = true,
                                string exceptionMessage = "Wait operation timed out.")
        {
            var waitStartTime = DateTime.Now;
            while (true)
            {
                var expired = (DateTime.Now - waitStartTime).TotalSeconds > timeOutSeconds;
                try
                {
                    var result = func();
                    if (result == WaitAction.StopWaiting)
                        return;
                    if (expired)
                        if (throwIfExpired)
                            throw new InvalidOperationException(exceptionMessage);
                        else
                            return;
                }
                catch
                {
                    if (expired)
                        throw;
                }
                Thread.Sleep(500);
            }
        }

        public static bool Wait(TimeSpan timeout, [NotNull] Func<WaitAction> func)
        {
            var waitStartTime = DateTime.Now;
            while (true)
            {
                var result = func();
                if (result == WaitAction.StopWaiting)
                    return true;
                if (DateTime.Now - waitStartTime > timeout)
                    return false;
                Thread.Sleep(500);
            }
        }

        public static void SleepForTimeSpan(TimeSpan delay)
        {
            var stopwatch = Stopwatch.StartNew();
            while (stopwatch.Elapsed < delay.Add(TimeSpan.FromMilliseconds(16*3)))
                Thread.Sleep(TimeSpan.FromMilliseconds(16));
        }
    }
}
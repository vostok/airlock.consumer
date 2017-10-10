using System;
using System.Runtime.Loader;
using System.Threading;
using System.Threading.Tasks;
using Vostok.Logging;

namespace Vostok.AirlockConsumer
{
    public class ConsumerApplicationHost<TConsumerApp>
        where TConsumerApp : ConsumerApplication, new()
    {
        private readonly ManualResetEventSlim stopSignal = new ManualResetEventSlim();

        public void Run()
        {
            var log = Logging.Configure($"log/{typeof (TConsumerApp).Name}-{{Date}}.log");
            AppDomain.CurrentDomain.UnhandledException += (_, eventArgs) =>
            {
                log.Fatal("Unhandled exception in curreant AppDomain", (Exception) eventArgs.ExceptionObject);
                Environment.Exit(1);
            };
            TaskScheduler.UnobservedTaskException += (_, eventArgs) =>
            {
                log.Fatal("UnobservedTaskException", eventArgs.Exception);
                eventArgs.SetObserved();
                Environment.Exit(2);
            };
            Console.CancelKeyPress += (_, eventArgs) =>
            {
                log.Warn("Ctrl+C is pressed -> terminating...");
                stopSignal.Set();
                eventArgs.Cancel = true;
            };
            AssemblyLoadContext.Default.Unloading += assemblyLoadContext =>
            {
                log.Warn("AssemblyLoadContext.Default.Unloading event is fired -> terminating...");
                stopSignal.Set();
            };
            try
            {
                log.Info($"Consumer application started: {typeof (TConsumerApp).Name}");
                var consumerApplication = new TConsumerApp();
                var consumerGroupHost = consumerApplication.Initialize(log);
                consumerGroupHost.Start();
                stopSignal.Wait(Timeout.Infinite);
                consumerGroupHost.Stop();
                log.Info($"Consumer application stopped: {typeof (TConsumerApp).Name}");
            }
            catch (Exception e)
            {
                log.Fatal("Unhandled exception on the main thread", e);
                Environment.Exit(3);
            }
        }
    }
}
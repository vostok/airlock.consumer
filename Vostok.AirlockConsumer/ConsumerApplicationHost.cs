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
        private readonly ManualResetEventSlim terminationSignal = new ManualResetEventSlim();

        public void Run()
        {
            var log = Logging.Configure();
            AppDomain.CurrentDomain.UnhandledException += (_, eventArgs) =>
            {
                log.Fatal("Unhandled exception in curreant AppDomain", (Exception) eventArgs.ExceptionObject);
                Environment.Exit(1);
            };
            TaskScheduler.UnobservedTaskException += (_, eventArgs) =>
            {
                log.Fatal("UnobservedTaskException", eventArgs.Exception);
                eventArgs.SetObserved();
            };
            Console.CancelKeyPress += (_, eventArgs) =>
            {
                log.Info("Ctrl+C is pressed -> terminating...");
                stopSignal.Set();
                eventArgs.Cancel = true;
            };
            AssemblyLoadContext.Default.Unloading += assemblyLoadContext =>
            {
                log.Info("AssemblyLoadContext.Default.Unloading event is fired -> terminating...");
                stopSignal.Set();
                terminationSignal.Wait(Timeout.Infinite);
                log.Info("Termination signal is set -> exiting...");
                Environment.Exit(0);
            };
            try
            {
                log.Info($"Consumer application is starting: {typeof (TConsumerApp).Name}");
                using (var consumerApplication = new TConsumerApp())
                {
                    var environmentVariables = AirlockEnvironmentVariables.CreateFromProcessEnvironment(log);
                    var consumerGroupHost = consumerApplication.Initialize(log, environmentVariables);
                    log.Info($"Consumer application is initialized: {typeof (TConsumerApp).Name}");
                    consumerGroupHost.Start();
                    stopSignal.Wait(Timeout.Infinite);
                    log.Info($"Stopping consumer group host for: {typeof (TConsumerApp).Name}");
                    consumerGroupHost.Stop();
                }
                log.Info($"Consumer application is stopped: {typeof (TConsumerApp).Name}");
                terminationSignal.Set();
            }
            catch (Exception e)
            {
                log.Fatal("Unhandled exception on the main thread", e);
                Environment.Exit(3);
            }
        }

        public void Stop()
        {
            stopSignal.Set();
        }
    }
}
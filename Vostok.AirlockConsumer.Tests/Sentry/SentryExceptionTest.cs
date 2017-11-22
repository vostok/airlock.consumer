using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using NUnit.Framework;
using SharpRaven.Data;
using Vostok.AirlockConsumer.Sentry;
using Vostok.Logging;
using Vostok.Logging.Logs;

namespace Vostok.AirlockConsumer.Tests.Sentry
{
    public class SentryExceptionTest
    {
        private readonly ConsoleLog log = new ConsoleLog();
        private readonly VostokRavenClient vostokRavenClient;

        public SentryExceptionTest()
        {
            vostokRavenClient = new VostokRavenClient("http://88136054931b49089e319c8db2f8330c:30c7a8a830b142cc8270d544184ef0b8@vostok-sentry:9000/2", log);
            vostokRavenClient.BeforeSend = r =>
            {
                log.Debug(r.Packet.ToPrettyJson());
                return r;
            };
        }

        private void MyGenericFunc<T>()
        {
            throw new Exception("hello from generic func " + typeof(T).Name);
        }

        private class MyClass<T>
        {
            public void MyFunc()
            {
                throw new Exception("hello from generic class " + typeof(T).Name);
            }
        }

        private async Task MyAsyncFunc()
        {
            await Task.Delay(200);
            if (vostokRavenClient != null)
                throw new Exception("hello from async func");
            await Task.Delay(200);
        }

        private void MyLambdaFunc()
        {
            var action = new Action(
                () => throw new Exception("hello from lambda"));
            action();
        }

        private void NestedFunc()
        {
            try
            {
                NestedFunc2();
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("invalid oper", e);
            }
        }

        private void NestedFunc2()
        {
            throw new InvalidDataException("bad data");
        }

        [Test]
        public void Test()
        {
            //try
            //{
            //    int i2 = 0;
            //    // ReSharper disable once UnusedVariable
            //    int i = 10 / i2;
            //}
            //catch (Exception exception)
            //{
            //    CaptureException(exception);
            //}
            //try
            //{
            //    MyGenericFunc<int>();
            //}
            //catch (Exception e)
            //{
            //    CaptureException(e);
            //}
            //try
            //{
            //    new MyClass<double>().MyFunc();
            //}
            //catch (Exception e)
            //{
            //    CaptureException(e);
            //}
            //try
            //{
            //    MyAsyncFunc().GetAwaiter().GetResult();
            //}
            //catch (Exception e)
            //{
            //    CaptureException(e);
            //}
            //try
            //{
            //    MyLambdaFunc();
            //}
            //catch (Exception e)
            //{
            //    CaptureException(e);
            //}
            try
            {
                NestedFunc();
            }
            catch (Exception e)
            {
                CaptureException(e);
            }

        }

        private void CaptureException(Exception exception)
        {
            log.Error(exception);
            vostokRavenClient.Capture(new SentryEvent(exception));
            vostokRavenClient.Capture(
                new SentryEvent(exception.Message)
                {
                    Tags = new Dictionary<string, string>
                    {
                        ["exception"] = exception.ToString(),
                        ["timestamp"] = DateTimeOffset.UtcNow.ToString("O")
                    },
                    Level = ErrorLevel.Error,
                });
        }
    }
}
using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FluentAssertions;
using NUnit.Framework;
using Vostok.AirlockConsumer.Sentry;
using Vostok.Logging;
using Vostok.Logging.Logs;

namespace Vostok.AirlockConsumer.Tests.Sentry
{
    public class ExceptionParsingTests
    {
        private readonly ConsoleLog log = new ConsoleLog();
        private readonly ExceptionParser exceptionParser = new ExceptionParser();

        private static void MyGenericFunc<T>()
        {
            throw new Exception("hello from generic func " + typeof (T).Name);
        }

        private class MyClass<T>
        {
            public void MyFunc()
            {
                throw new Exception("hello from generic class " + typeof (T).Name);
            }
        }

        private static async Task MyAsyncFunc()
        {
            await Task.Delay(200);
            throw new Exception("hello from async func");
        }

        private static void MyLambdaFunc()
        {
            var action = new Action(
                () => throw new Exception("hello from lambda"));
            action();
        }

        private static void NestedFunc()
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

        private static void NestedFunc2()
        {
            throw new InvalidDataException("bad data");
        }

        private static void DivideByZero()
        {
            int i2 = 0;
            // ReSharper disable once UnusedVariable
            int i = 10/i2;
        }

        private static readonly object[] testCases =
        {
            new object[] {(Action) DivideByZero, new[] {"TestByThrowingException", "DivideByZero"}, new []{ "DivideByZeroException" } },
            new object[] {(Action) MyGenericFunc<int>, new[] {"TestByThrowingException", "MyGenericFunc"}, new []{ "Exception" } },
            new object[] {(Action) (() => new MyClass<double>().MyFunc()), new[] {"TestByThrowingException", "cctor { <lambda> }", "MyFunc"}, new []{ "Exception" } },
            new object[] {(Action) (() => MyAsyncFunc().GetAwaiter().GetResult()), new[] {"TestByThrowingException", "cctor { <lambda> }", "HandleNonSuccessAndDebuggerNotification", "Throw", "MyAsyncFunc"}, new []{ "Exception" } },
            new object[] {(Action) MyLambdaFunc, new[] {"TestByThrowingException", "MyLambdaFunc", "MyLambdaFunc { <lambda> }"}, new []{ "Exception" } },
            new object[] {(Action) NestedFunc, new[] {"TestByThrowingException", "NestedFunc", "NestedFunc", "NestedFunc2"}, new []{ "InvalidOperationException", "InvalidDataException" } }
        };

        [TestCaseSource(nameof(testCases))]
        public void TestByThrowingException(Action action, string[] funcNames, string[] exNames) //Action action
        {
            try
            {
                action();
            }
            catch (Exception e)
            {
                log.Error(e);
                var sentryExceptions = exceptionParser.Parse(e.ToString());
                //var jsonPacket = new JsonPacket("vostok", exception);
                //log.Debug("expected:\n" + jsonPacket.Exceptions.ToPrettyJson());
                log.Debug("got:\n" + sentryExceptions.ToPrettyJson());
                sentryExceptions.SelectMany(e1 => e1.Stacktrace.Frames).Select(x => x.Function).ShouldAllBeEquivalentTo(funcNames);
                sentryExceptions.Select(ex => ex.Type).ShouldAllBeEquivalentTo(exNames);
            }
        }

        private static readonly object[] preparedTextTestCases =
        {
            new object[] {rusEx, new[] {"Call", "Call { <lambda> }", "PerformHttpRequestInternal { <lambda> }", "<>n__FabricatedMethod3", "PerformHttpRequest" }, new []{ "HttpClientException", "WebException", "SocketException" } },
            new object[] {bigEx, new[] {"Call", "Call { <lambda> }", "GetJsonResult", "PerformJsonHttpRequest", "PerformJsonHttpRequest" }, new[] { "AttemptsExceededException", "ConnectorHttpClientException" } }
        };

        //[TestCaseSource(nameof(preparedTextTestCases))] //не смог заставить работать через TestCaseSource
        [Test]
        public void PreparedTextTest() //string text, string[] funcNames
        {
            foreach (var testCase in preparedTextTestCases.Cast<object[]>())
            {
                var text = (string) testCase[0]; //, string[] funcNames
                var funcNames = (string[]) testCase[1];
                var exNames = (string[])testCase[2];
                var sentryExceptions = exceptionParser.Parse(text);
                log.Debug("got:\n" + sentryExceptions.ToPrettyJson());
                sentryExceptions.SelectMany(e => e.Stacktrace.Frames).Select(x => x.Function).Take(5).ShouldAllBeEquivalentTo(funcNames);
                sentryExceptions.Select(ex => ex.Type).ShouldAllBeEquivalentTo(exNames);
            }
        }

        private const string rusEx = @"Diadoc.Api.Http.HttpClientException: BaseUrl=http://localhost:27183, PathAndQuery=/GetBox?boxId=9f9e1bd9-54ac-4896-aa9d-b143f9a38427, AdditionalMessage=, StatusCode=, DiadocErrorCode:  ---> System.Net.WebException: Невозможно соединиться с удаленным сервером ---> System.Net.Sockets.SocketException: Подключение не установлено, т.к. конечный компьютер отверг запрос на подключение 127.0.0.1:27183
   в System.Net.Sockets.Socket.DoConnect(EndPoint endPointSnapshot, SocketAddress socketAddress)
   в System.Net.ServicePoint.ConnectSocketInternal(Boolean connectFailure, Socket s4, Socket s6, Socket& socket, IPAddress& address, ConnectSocketState state, IAsyncResult asyncResult, Exception& exception)
   --- Конец трассировки внутреннего стека исключений ---
   в System.Net.HttpWebRequest.GetResponse()
   в Diadoc.Api.Http.HttpClient.PerformHttpRequest(HttpRequest request, HttpStatusCode[] allowedStatusCodes) в c:\Dev2\diadocsdk\C#\DiadocApi\Http\HttpClient.cs:строка 93
   --- Конец трассировки внутреннего стека исключений ---
   в Diadoc.Api.Http.HttpClient.PerformHttpRequest(HttpRequest request, HttpStatusCode[] allowedStatusCodes) в c:\Dev2\diadocsdk\C#\DiadocApi\Http\HttpClient.cs:строка 122
   в DC.Http.ExtendedHttpClient.<>n__FabricatedMethod3(HttpRequest , HttpStatusCode[] )
   в DC.Http.ExtendedHttpClient.<>c__DisplayClass1.<PerformHttpRequestInternal>b__0() в c:\Sources\diadocconnector\_Src\Core\Common\DC.Http\ExtendedHttpClient.cs:строка 39
   в DC.CallStrategy.RetriableCallStrategyBase.<>c__DisplayClass1`1.<Call>b__0() в c:\Sources\diadocconnector\_Src\Core\Common\DC.CallStrategy\IRetriableCallStrategy.cs:строка 19
   в DC.CallStrategy.MaxAttemptsRetriableCallStrategy.Call(Action action, Func`2 isExceptionRetriable, Action beforeRetry) в c:\Sources\diadocconnector\_Src\Core\Common\DC.CallStrategy\MaxAttemptsRetriableCallStrategy.cs:строка 41";

        private const string bigEx = @"DC.CallStrategy.AttemptsExceededException: ResponseStatusCode: BadGateway
RequestUrl: https://billy-publicapi.kontur.ru/subscriptions/actual?productCode=7&recipientId=61594e6f-67a4-428a-9d7b-392c4d89bbb9
<html>
<head><title>502 Bad Gateway</title></head>
<body bgcolor = white >
< center >< h1 > 502 Bad Gateway</h1></center>
<hr><center>nginx</center>
</body>
</html>
 ---> DC.Core.Http.ConnectorHttpClientException: ResponseStatusCode: BadGateway
RequestUrl: https://billy-publicapi.kontur.ru/subscriptions/actual?productCode=7&recipientId=61594e6f-67a4-428a-9d7b-392c4d89bbb9
<html>
<head><title>502 Bad Gateway</title></head>
<body bgcolor = white >
< center >< h1 > 502 Bad Gateway</h1></center>
<hr><center>nginx</center>
</body>
</html>

   at DiadocSys.Net.Http.Client.HttpResponseExtensions.CheckStatusCode(IHttpResponse response, HttpStatusCode[] allowedStatusCodes) in C:\cement\diadocconnector\_Src\Connector.Cloud\Core\DC.CloudCore\DiadocSys\Net.Http\Client\HttpResponseExtensions.cs:line 24
   at DiadocSys.Net.Http.Client.HttpClusterClientExtensions.PerformHttpRequest[TResponse](IHttpClusterClient httpClient, IHttpRequest httpRequest, Nullable`1 clientId, Func`2 parseResponse) in C:\cement\diadocconnector\_Src\Connector.Cloud\Core\DC.CloudCore\DiadocSys\Net.Http\Client\HttpClusterClientExtensions.cs:line 79
   at DiadocSys.Net.Http.Client.HttpClusterClientExtensions.PerformJsonHttpRequest[TResponse](IHttpClusterClient httpClient, IHttpRequest httpRequest, Nullable`1 clientId) in C:\cement\diadocconnector\_Src\Connector.Cloud\Core\DC.CloudCore\DiadocSys\Net.Http\Client\HttpClusterClientExtensions.cs:line 46
   at DC.Billing.Api.HttpClusterClientExtensions.PerformJsonHttpRequest[TResponse](IHttpClusterClient httpClient, IRequestContext requestContext, String queryString, HttpMethod httpMethod, KeyValuePair`2[] headers, Nullable`1 clientId) in C:\cement\diadocconnector\_Src\Connector.Cloud\Core\DC.CloudCore\Billing\Api\HttpClusterClientExtensions.cs:line 23
   at DC.Billing.Api.BillingClient.GetJsonResult[TResponse](IRequestContext requestContext, String queryString) in C:\cement\diadocconnector\_Src\Connector.Cloud\Core\DC.CloudCore\Billing\Api\BillingClient.cs:line 61
   at DC.CallStrategy.RetriableCallStrategyBase.<>c__DisplayClass1_0`1.<Call>b__0() in C:\cement\diadocconnector\_Src\Connector.Core\DC.Core\CallStrategy\IRetriableCallStrategy.cs:line 19
   at DC.CallStrategy.MaxAttemptsRetriableCallStrategy.Call(Action action, Func`2 isExceptionRetriable, Action beforeRetry) in C:\cement\diadocconnector\_Src\Connector.Core\DC.Core\CallStrategy\MaxAttemptsRetriableCallStrategy.cs:line 41
   --- End of inner exception stack trace ---
   at DC.CallStrategy.MaxAttemptsRetriableCallStrategy.Call(Action action, Func`2 isExceptionRetriable, Action beforeRetry) in C:\cement\diadocconnector\_Src\Connector.Core\DC.Core\CallStrategy\MaxAttemptsRetriableCallStrategy.cs:line 58
   at DC.CallStrategy.RetriableCallStrategyBase.Call[T](Func`1 action, Func`2 isExceptionRetriable, Action beforeRetry) in C:\cement\diadocconnector\_Src\Connector.Core\DC.Core\CallStrategy\IRetriableCallStrategy.cs:line 20
   at DC.Billing.Api.BillingClient.GetActualSubscriptionsByProduct(IRequestContext requestContext, Guid accountId, String productCode) in C:\cement\diadocconnector\_Src\Connector.Cloud\Core\DC.CloudCore\Billing\Api\BillingClient.cs:line 56
   at DC.Billing.Api.BillingClient.GetActualSubscriptions(IRequestContext requestContext, Guid accountId) in C:\cement\diadocconnector\_Src\Connector.Cloud\Core\DC.CloudCore\Billing\Api\BillingClient.cs:line 41
   at DC.Billing.AuthorizationService.GetActualAuthorizationInfo(ConnectorBillingEntity connectorBilling, String billingId) in C:\cement\diadocconnector\_Src\Connector.Cloud\Core\DC.CloudCore\Billing\AuthorizationService.cs:line 139
   at DC.Billing.AuthorizationService.GetAuthorizationInfo(String connectorInstanceId, ConnectorBillingEntity connectorBilling, String billingId) in C:\cement\diadocconnector\_Src\Connector.Cloud\Core\DC.CloudCore\Billing\AuthorizationService.cs:line 119
   at DC.Billing.MonitoredAuthorizationService.<>c__DisplayClass6_0.<GetAuthorizationInfo>b__1() in C:\cement\diadocconnector\_Src\Connector.Cloud\Core\DC.CloudCore\Billing\MonitoredAuthorizationService.cs:line 39
   at DC.Billing.MonitoredAuthorizationService.PerformAndMeasureAction[T](String metricName, Func`1 action, Func`2 transformResultToMetric) in C:\cement\diadocconnector\_Src\Connector.Cloud\Core\DC.CloudCore\Billing\MonitoredAuthorizationService.cs:line 66
   at DC.Billing.MonitoredAuthorizationService.GetAuthorizationInfo(String connectorInstanceId, ConnectorBillingEntity connectorBilling, String billingId) in C:\cement\diadocconnector\_Src\Connector.Cloud\Core\DC.CloudCore\Billing\MonitoredAuthorizationService.cs:line 39
   at DC.Billing.AuthorizationService.PerformAuthorization(String connectorInstanceId, String boxId) in C:\cement\diadocconnector\_Src\Connector.Cloud\Core\DC.CloudCore\Billing\AuthorizationService.cs:line 86
   at DC.Billing.MonitoredAuthorizationService.<>c__DisplayClass5_0.<PerformAuthorization>b__0() in C:\cement\diadocconnector\_Src\Connector.Cloud\Core\DC.CloudCore\Billing\MonitoredAuthorizationService.cs:line 33
   at DC.Billing.MonitoredAuthorizationService.PerformAndMeasureAction[T](String metricName, Func`1 action, Func`2 transformResultToMetric) in C:\cement\diadocconnector\_Src\Connector.Cloud\Core\DC.CloudCore\Billing\MonitoredAuthorizationService.cs:line 66
   at DC.Api.Handlers.StartConnectorHandler.EnsureBillingAuthorized(NameValueCollection parameters, ConnectorInstanceEntity connector) in C:\cement\diadocconnector\_Src\Connector.Cloud\Cloud\Api\DC.Api\Handlers\StartConnectorHandler.cs:line 142
   at DC.Api.Handlers.StartConnectorHandler.ProcessRequestFromConnectorInstance(IDCRequestContext requestContext, NameValueCollection parameters, ConnectorInstanceEntity connectorInstance, Byte[] requestBody) in C:\cement\diadocconnector\_Src\Connector.Cloud\Cloud\Api\DC.Api\Handlers\StartConnectorHandler.cs:line 87
   at DC.Api.Common.ConnectorInstanceBasedHandler.ProcessRequestFromOrganization(IDCRequestContext requestContext, NameValueCollection parameters, ConnectorOrganizationEntity connectorOrganization, Byte[] requestBody) in C:\cement\diadocconnector\_Src\Connector.Cloud\Cloud\Api\DC.Api.Common\ConnectorInstanceBasedHandler.cs:line 61
   at DC.Api.Common.OrganizationBasedHandler.ProcessRequestAuthorized(IDCRequestContext requestContext, NameValueCollection parameters, Byte[] requestBody) in C:\cement\diadocconnector\_Src\Connector.Cloud\Cloud\Api\DC.Api.Common\OrganizationBasedHandler.cs:line 49
   at DC.Api.Common.BasicApiHandler.ProcessRequest(IHttpContext httpContext) in C:\cement\diadocconnector\_Src\Connector.Cloud\Cloud\Api\DC.Api.Common\BasicApiHandler.cs:line 56
---> (Inner Exception #0) DC.Core.Http.ConnectorHttpClientException: ResponseStatusCode: BadGateway
RequestUrl: https://billy-publicapi.kontur.ru/subscriptions/actual?productCode=7&recipientId=61594e6f-67a4-428a-9d7b-392c4d89bbb9
<html>
<head><title>502 Bad Gateway</title></head>
<body bgcolor=white>
<center><h1>502 Bad Gateway</h1></center>
<hr><center>nginx</center>
</body>
</html>

   at DiadocSys.Net.Http.Client.HttpResponseExtensions.CheckStatusCode(IHttpResponse response, HttpStatusCode[] allowedStatusCodes) in C:\cement\diadocconnector\_Src\Connector.Cloud\Core\DC.CloudCore\DiadocSys\Net.Http\Client\HttpResponseExtensions.cs:line 24
   at DiadocSys.Net.Http.Client.HttpClusterClientExtensions.PerformHttpRequest[TResponse](IHttpClusterClient httpClient, IHttpRequest httpRequest, Nullable`1 clientId, Func`2 parseResponse) in C:\cement\diadocconnector\_Src\Connector.Cloud\Core\DC.CloudCore\DiadocSys\Net.Http\Client\HttpClusterClientExtensions.cs:line 79
   at DiadocSys.Net.Http.Client.HttpClusterClientExtensions.PerformJsonHttpRequest[TResponse](IHttpClusterClient httpClient, IHttpRequest httpRequest, Nullable`1 clientId) in C:\cement\diadocconnector\_Src\Connector.Cloud\Core\DC.CloudCore\DiadocSys\Net.Http\Client\HttpClusterClientExtensions.cs:line 46
   at DC.Billing.Api.HttpClusterClientExtensions.PerformJsonHttpRequest[TResponse](IHttpClusterClient httpClient, IRequestContext requestContext, String queryString, HttpMethod httpMethod, KeyValuePair`2[] headers, Nullable`1 clientId) in C:\cement\diadocconnector\_Src\Connector.Cloud\Core\DC.CloudCore\Billing\Api\HttpClusterClientExtensions.cs:line 23
   at DC.Billing.Api.BillingClient.GetJsonResult[TResponse](IRequestContext requestContext, String queryString) in C:\cement\diadocconnector\_Src\Connector.Cloud\Core\DC.CloudCore\Billing\Api\BillingClient.cs:line 61
   at DC.CallStrategy.RetriableCallStrategyBase.<>c__DisplayClass1_0`1.<Call>b__0() in C:\cement\diadocconnector\_Src\Connector.Core\DC.Core\CallStrategy\IRetriableCallStrategy.cs:line 19
   at DC.CallStrategy.MaxAttemptsRetriableCallStrategy.Call(Action action, Func`2 isExceptionRetriable, Action beforeRetry) in C:\cement\diadocconnector\_Src\Connector.Core\DC.Core\CallStrategy\MaxAttemptsRetriableCallStrategy.cs:line 41<---

---> (Inner Exception #1) DC.Core.Http.ConnectorHttpClientException: ResponseStatusCode: GatewayTimeout
RequestUrl: https://billy-publicapi.kontur.ru/subscriptions/actual?productCode=7&recipientId=61594e6f-67a4-428a-9d7b-392c4d89bbb9
<html>
<head><title>504 Gateway Time-out</title></head>
<body bgcolor=white>
<center><h1>504 Gateway Time-out</h1></center>
<hr><center>nginx</center>
</body>
</html>

   at DiadocSys.Net.Http.Client.HttpResponseExtensions.CheckStatusCode(IHttpResponse response, HttpStatusCode[] allowedStatusCodes) in C:\cement\diadocconnector\_Src\Connector.Cloud\Core\DC.CloudCore\DiadocSys\Net.Http\Client\HttpResponseExtensions.cs:line 24
   at DiadocSys.Net.Http.Client.HttpClusterClientExtensions.PerformHttpRequest[TResponse](IHttpClusterClient httpClient, IHttpRequest httpRequest, Nullable`1 clientId, Func`2 parseResponse) in C:\cement\diadocconnector\_Src\Connector.Cloud\Core\DC.CloudCore\DiadocSys\Net.Http\Client\HttpClusterClientExtensions.cs:line 79
   at DiadocSys.Net.Http.Client.HttpClusterClientExtensions.PerformJsonHttpRequest[TResponse](IHttpClusterClient httpClient, IHttpRequest httpRequest, Nullable`1 clientId) in C:\cement\diadocconnector\_Src\Connector.Cloud\Core\DC.CloudCore\DiadocSys\Net.Http\Client\HttpClusterClientExtensions.cs:line 46
   at DC.Billing.Api.HttpClusterClientExtensions.PerformJsonHttpRequest[TResponse](IHttpClusterClient httpClient, IRequestContext requestContext, String queryString, HttpMethod httpMethod, KeyValuePair`2[] headers, Nullable`1 clientId) in C:\cement\diadocconnector\_Src\Connector.Cloud\Core\DC.CloudCore\Billing\Api\HttpClusterClientExtensions.cs:line 23
   at DC.Billing.Api.BillingClient.GetJsonResult[TResponse](IRequestContext requestContext, String queryString) in C:\cement\diadocconnector\_Src\Connector.Cloud\Core\DC.CloudCore\Billing\Api\BillingClient.cs:line 61
   at DC.CallStrategy.RetriableCallStrategyBase.<>c__DisplayClass1_0`1.<Call>b__0() in C:\cement\diadocconnector\_Src\Connector.Core\DC.Core\CallStrategy\IRetriableCallStrategy.cs:line 19
   at DC.CallStrategy.MaxAttemptsRetriableCallStrategy.Call(Action action, Func`2 isExceptionRetriable, Action beforeRetry) in C:\cement\diadocconnector\_Src\Connector.Core\DC.Core\CallStrategy\MaxAttemptsRetriableCallStrategy.cs:line 41<---
";
    }
}
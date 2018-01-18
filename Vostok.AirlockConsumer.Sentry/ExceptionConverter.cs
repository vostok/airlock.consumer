using System.Linq;
using SharpRaven.Data;
using Vostok.Airlock.Logging;

namespace Vostok.AirlockConsumer.Sentry
{
    public static class ExceptionConverter
    {
        public static SentryException ToSentry(this LogEventException ex)
        {
            return new SentryException(null)
            {
                Value = ex.Message,
                Module = ex.Module,
                Type = ex.Type,
                Stacktrace = new SentryStacktrace(null) {Frames = ex.Stack.Select(ToSentry).ToArray() }
            };
        }

        private static ExceptionFrame ToSentry(this LogEventStackFrame frame)
        {
            return new ExceptionFrame(null)
            {
                Module = frame.Module,
                Filename = frame.Filename,
                ColumnNumber = frame.ColumnNumber,
                LineNumber = frame.LineNumber,
                Function = frame.Function,
                InApp = !IsSystemModuleName(frame.Module),
                Source = frame.Source
            };
        }

        private static bool IsSystemModuleName(string moduleName)
        {
            return !string.IsNullOrEmpty(moduleName) &&
                   (moduleName.StartsWith("System.", System.StringComparison.Ordinal) ||
                    moduleName.StartsWith("Microsoft.", System.StringComparison.Ordinal));
        }
    }
}
using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;
using SharpRaven.Data;

namespace Vostok.AirlockConsumer.Sentry
{
    public class ExceptionParser
    {
        private readonly Regex stackFrameRegex = new Regex(@"^\s*(?:at|в)\s+(.*?)(?:\s+(?:in|в)\s+(.*):(?:line|строка)\s+(\d+).*)?\s*$");
        private readonly Regex asyncRegex = new Regex(@"^(.*)\.<(\w*)>d__\d*");
        private readonly Regex lambdaRegex = new Regex(@"^<?(\w*)>b__\w+");

        public List<SentryException> Parse(string fullExceptionStr)
        {
            if (string.IsNullOrEmpty(fullExceptionStr))
                return null;
            var lines = fullExceptionStr.Split('\n');
            if (lines.Length == 0)
                return null;
            var lineIndex = 0;
            var head = new StringBuilder();
            while (lineIndex < lines.Length)
            {
                if (stackFrameRegex.IsMatch(lines[lineIndex]))
                    break;
                head.Append(lines[lineIndex++]);
            }
            var headStr = head.ToString();
            var sentryExceptions = BuildExceptionListFromHeader(headStr);
            if (sentryExceptions.Count == 0)
                return null;
            var frames = new List<ExceptionFrame>();
            var currentExIndex = sentryExceptions.Count - 1;
            while (lineIndex < lines.Length)
            {
                var line = lines[lineIndex++];
                if (line.Contains("--- End of inner exception stack trace ---") || line.Contains("--- Конец трассировки внутреннего стека исключений ---"))
                {
                    if (currentExIndex > 0)
                    {
                        AddFramesToException(sentryExceptions[currentExIndex], frames);
                        currentExIndex--;
                    }
                }
                else
                {
                    var match = stackFrameRegex.Match(line);
                    if (!match.Success)
                        continue;
                    int.TryParse(match.Groups[3].Value, out var lineNumber);
                    (var module, var name, var source) = ParseFullName(match.Groups[1].Value);
                    frames.Add(
                        new ExceptionFrame(null)
                        {
                            Function = name,
                            Module = module,
                            Filename = match.Groups[2].Value,
                            LineNumber = lineNumber,
                            Source = source,
                            InApp = lineNumber > 0
                        });
                }
            }
            AddFramesToException(sentryExceptions[currentExIndex], frames);
            return sentryExceptions;
        }

        private List<SentryException> BuildExceptionListFromHeader(string headStr)
        {
            var sentryExceptions = new List<SentryException>();
            var exceptions = headStr.Split(" ---> ");
            foreach (var exStr in exceptions)
            {
                var messageIndex = exStr.IndexOf(":", StringComparison.Ordinal);
                var exception = new SentryException(null);
                if (messageIndex >= 0)
                {
                    (_, exception.Type, _) = ParseFullName(exStr.Substring(0, messageIndex));
                    exception.Value = exStr.Substring(messageIndex + 1).Trim();
                }
                else
                {
                    exception.Value = exStr;
                }
                sentryExceptions.Add(exception);
            }
            return sentryExceptions;
        }

        private void AddFramesToException(SentryException exception, List<ExceptionFrame> frames)
        {
            frames.Reverse();
            exception.Stacktrace = new SentryStacktrace(null) {Frames = frames.ToArray()};
            if (frames.Count > 0)
                exception.Module = frames[0].Module;
            frames.Clear();
        }

        private (string module, string name, string source) ParseFullName(string s)
        {
            var dotPos = s.LastIndexOf(".", StringComparison.Ordinal);
            string module;
            string name;
            if (dotPos >= 0)
            {
                module = s.Substring(0, dotPos);
                name = s.Substring(dotPos + 1);
            }
            else
            {
                name = s;
                module = "";
            }
            var source = name;
            var nameEnd = name.IndexOf("(", StringComparison.Ordinal);
            if (nameEnd > 0)
                name = name.Substring(0, nameEnd);
            nameEnd = name.IndexOf("[", StringComparison.Ordinal);
            if (nameEnd > 0)
                name = name.Substring(0, nameEnd);
            if (name == "MoveNext")
            {
                var asyncMatch = asyncRegex.Match(module);
                if (asyncMatch.Success)
                {
                    module = asyncMatch.Groups[1].Value;
                    name = asyncMatch.Groups[2].Value;
                }
            }
            var matchLambda = lambdaRegex.Match(name);
            if (matchLambda.Success)
            {
                name = matchLambda.Groups[1].Value + " { <lambda> }";
            }
            if (module.EndsWith(".<"))
                module = module.Substring(0, module.Length - 2);
            if (module.EndsWith(".<>c"))
                module = module.Substring(0, module.Length - 4);
            return (module, name, source);
        }
    }
}
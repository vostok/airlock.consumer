using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
using SharpRaven.Data;

namespace Vostok.AirlockConsumer.Sentry
{
    internal class VostokJsonPacketFactory : JsonPacketFactory
    {
        protected override JsonPacket OnCreate(JsonPacket jsonPacket)
        {
            jsonPacket.ServerName = null;
            jsonPacket.User = new SentryUser("");
            jsonPacket.Modules.Clear();
            if (jsonPacket.Tags.TryGetValue("exception", out var exceptionStr))
            {
                ParseException(jsonPacket, exceptionStr);
                jsonPacket.Tags.Remove("exception");
            }
            if (jsonPacket.Tags.TryGetValue("timestamp", out var timestampStr))
            {
                if (!string.IsNullOrEmpty(exceptionStr))
                    jsonPacket.TimeStamp = DateTime.ParseExact(timestampStr, "O", CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal);
                jsonPacket.Tags.Remove("timestamp");
            }
            return jsonPacket;
        }

        private readonly Regex stackFrameRegex = new Regex(@"^\s*at\s+(.*?)(?:\s+in\s+(.*):line\s+(\d+))?\s*$", RegexOptions.Compiled);

        private void ParseException(JsonPacket jsonPacket, string exceptionStr)
        {
            if (string.IsNullOrEmpty(exceptionStr))
                return;
            var lines = exceptionStr.Split('\n');
            if (lines.Length == 0)
                return;
            var head = lines[0];
            var messageIndex = head.IndexOf(":", StringComparison.Ordinal);
            var exception = new SentryException(null);
            if (messageIndex >= 0)
            {
                (exception.Module, exception.Type, _) = ParseFullName(head.Substring(0, messageIndex));
                exception.Value = head.Substring(messageIndex + 1).Trim();
            }
            else
            {
                exception.Value = head;
            }
            var frames = new List<ExceptionFrame>();
            foreach (var line in lines.Skip(1))
            {
                var match = stackFrameRegex.Match(line);
                if (!match.Success)
                    continue;
                int.TryParse(match.Groups[3].Value, out var lineNumber);
                (var module, var name, var source) = ParseFullName(match.Groups[1].Value);
                frames.Add(new ExceptionFrame(null)
                {
                    Function = name,
                    Module = module,
                    AbsolutePath = match.Groups[2].Value,
                    LineNumber = lineNumber,
                    Source = source
                });
            }
            exception.Stacktrace = new SentryStacktrace(null) { Frames = frames.ToArray() };
            jsonPacket.Exceptions = new List<SentryException> { exception };
        }

        private static (string module, string name, string source) ParseFullName(string s)
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
            return (module, name, source);
        }
    }
}
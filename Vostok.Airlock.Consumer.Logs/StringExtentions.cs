using System;

namespace Vostok.Airlock.Consumer.Logs
{
    internal static class StringExtentions
    {
        private const string pattern = "{0}[truncated {1}]";

        public static string Truncate(this string source, int length)
        {
            if (source.Length <= length)
                return source;

            var x = source.Length - length;
            var y = pattern.Length - 6 + GetNumberOfDigits(x);
            var truncatedCount = x + y + (GetNumberOfDigits(x + y) > GetNumberOfDigits(x) ? 1 : 0);
            return string.Format(pattern, source.Substring(0, source.Length - truncatedCount), truncatedCount);
        }

        private static int GetNumberOfDigits(int i)
        {
            return (int)Math.Floor(Math.Log10(i) + 1);
        }
    }
}
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Text;

namespace TrackDb.Lib
{
    /// <summary>
    /// Extensions LINQ methods
    /// </summary>
    internal static class EnumerableHelper
    {
        /// <summary>Cap the number of elements so the sum of values reaches a value.</summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="enumerable"></param>
        /// <param name="valueSelector"></param>
        /// <param name="capValue"></param>
        /// <returns></returns>
        public static IEnumerable<T> CapSumValues<T>(
            this IEnumerable<T> enumerable,
            Func<T, int> valueSelector,
            int capValue)
        {
            var builder = ImmutableArray<T>.Empty.ToBuilder();
            var sumValue = 0;

            foreach (var item in enumerable)
            {
                var value = valueSelector(item);

                builder.Add(item);
                sumValue += value;
                if (sumValue >= capValue)
                {
                    break;
                }
            }

            return builder.ToImmutable();
        }
    }
}
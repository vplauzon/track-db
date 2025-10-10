using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading.Tasks;

namespace TrackDb.Lib
{
    internal static class AsyncEnumerableHelper
    {
        public static async Task<IImmutableList<T>> ToImmutableListAsync<T>(
            this IAsyncEnumerable<T> asyncEnumerable)
        {
            var builder = ImmutableArray<T>.Empty.ToBuilder();

            await foreach(var item in asyncEnumerable)
            {
                builder.Add(item);
            }

            return builder.ToImmutable();
        }
    }
}
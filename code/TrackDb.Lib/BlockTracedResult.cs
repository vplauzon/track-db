using System.Collections.Generic;

namespace TrackDb.Lib
{
    internal record struct BlockTracedResult<T>(
        IReadOnlyList<BlockTrace> BlockTraces,
        T Result);
}
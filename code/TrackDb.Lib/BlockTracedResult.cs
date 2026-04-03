using System;
using System.Collections.Generic;

namespace TrackDb.Lib
{
    internal record struct BlockTracedResult(
        IReadOnlyList<BlockTrace> BlockTraces,
        ReadOnlyMemory<object?> Result);
}
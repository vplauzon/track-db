using System;
using System.Collections.Generic;
using System.Text;

namespace TrackDb.Lib.InMemory.Block
{
    internal interface ITypedReadOnlyDataColumn<T>
    {
        ReadOnlySpan<T> RecordValues { get; }
    }
}
using System;
using System.Buffers.Binary;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using TrackDb.Lib.InMemory.Block.SpecializedColumn;
using TrackDb.Lib.Encoding;

namespace TrackDb.Lib.InMemory.Block
{
    /// <summary>
    /// Represents a serialized in-memory block with statistics.
    /// </summary>
    /// <remarks>
    /// Statistics are available when a block is serialized but isn't always persisted
    /// in tables.  For this reason, <see cref="SerializedBlock"/> is used to represent
    /// a block from a table.
    /// </remarks>
    /// <param name="ItemCount"></param>
    /// <param name="Size"></param>
    /// <param name="ColumnMinima"></param>
    /// <param name="ColumnMaxima"></param>
    /// <param name="Payload"></param>
    internal record StatsSerializedBlock(
        int ItemCount,
        int Size,
        IImmutableList<object?> ColumnMinima,
        IImmutableList<object?> ColumnMaxima,
        ReadOnlyMemory<byte> Payload)
    {
    }
}
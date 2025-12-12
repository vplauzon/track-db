using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace TrackDb.Lib.InMemory.Block
{
    /// <summary>
    /// Represents statistics of a block.
    /// </summary>
    /// <param name="ItemCount"></param>
    /// <param name="Size"></param>
    /// <param name="Columns"></param>
    internal record BlockStats(
        int ItemCount,
        int Size,
        IImmutableList<ColumnStats> Columns)
    {
        public static BlockStats Empty { get; } =
            new BlockStats(0, 0, ImmutableArray<ColumnStats>.Empty);

        public override string ToString()
        {
            return $"(Count={ItemCount}, Size={Size})";
        }
    }
}
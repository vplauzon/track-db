using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace TrackDb.Lib.InMemory.Block
{
    /// <summary>
    /// Metadata block generic (non schema specific) information.
    /// </summary>
    internal record MetadataBlock(
        MetadataTableSchema Schema,
        int BlockId,
        int ItemCount,
        int Size,
        long MinRecordId,
        long MaxRecordId)
    {
        public static IImmutableList<int> GetColumnIndexes(MetadataTableSchema Schema)
        {
            return [
                Schema.BlockIdColumnIndex,
                Schema.ItemCountColumnIndex,
                Schema.SizeColumnIndex,
                Schema.RecordIdMinColumnIndex,
                Schema.RecordIdMaxColumnIndex
                ];
        }

        public static MetadataBlock Create(
            MetadataTableSchema Schema,
            ReadOnlySpan<object?> metadataRecord)
        {
            return new(
                Schema,
                (int)metadataRecord[0]!,
                (int)metadataRecord[1]!,
                (int)metadataRecord[2]!,
                (long)metadataRecord[3]!,
                (long)metadataRecord[4]!);
        }
    }
}
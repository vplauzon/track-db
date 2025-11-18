using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace TrackDb.Lib.InMemory.Block
{
    /// <summary>
    /// Adapter to a metadata block.
    /// </summary>
    /// <param name="MetadataRecord"></param>
    /// <param name="Schema"></param>
    internal record MetaDataBlock(
        ReadOnlyMemory<object?> MetadataRecord,
        MetadataTableSchema Schema)
    {
        public int ItemCount => (int)MetadataRecord.Span[Schema.ItemCountColumnIndex]!;

        public int Size => (int)MetadataRecord.Span[Schema.SizeColumnIndex]!;
        
        public int BlockId => (int)MetadataRecord.Span[Schema.BlockIdColumnIndex]!;
    }
}
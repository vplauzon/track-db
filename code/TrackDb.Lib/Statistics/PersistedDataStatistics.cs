using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib.Statistics
{
    public record PersistedDataStatistics(int BlockCount, long RecordCount, long Size)
    {
        #region Constructor
        internal static PersistedDataStatistics Create(Table metaDataTable, TransactionContext tx)
        {
            var metadataSchema = (MetadataTableSchema)metaDataTable.Schema;
            var stats = metaDataTable.Query(tx)
                .WithProjection(metadataSchema.SizeColumnIndex, metadataSchema.ItemCountColumnIndex)
                .Select(r => new
                {
                    Size = (int)r.Span[0]!,
                    ItemCount = (int)r.Span[1]!
                })
                .Aggregate(
                new PersistedDataStatistics(0, 0, 0),
                (stats, meta) => new(
                    stats.BlockCount + 1,
                    stats.RecordCount + meta.ItemCount,
                    stats.Size + meta.Size));

            return stats;
        }
        #endregion

        public long BlockSize => BlockCount == 0 ? 0 : Size / BlockCount;

        public long RecordSize => RecordCount == 0 ? 0 : Size / RecordCount;

        public long RecordPerBlock => BlockCount == 0 ? 0 : RecordCount / BlockCount;
    }
}
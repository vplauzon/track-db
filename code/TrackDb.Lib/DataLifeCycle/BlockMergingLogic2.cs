using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using TrackDb.Lib.InMemory.Block;

namespace TrackDb.Lib.DataLifeCycle
{
    internal class BlockMergingLogic2 : LogicBase
    {
        #region Inner Types
        #endregion

        public BlockMergingLogic2(Database database)
            : base(database)
        {
        }

        /// <summary>
        /// Merge together blocks belonging to <paramref name="metaBlockId"/>.
        /// If <paramref name="tableName"/> is a data table, i.e. is at the lowest level,
        /// it will compact tombstoned records.
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="metaBlockId">If <c>null</c>, use in-memory meta block.</param>
        /// <param name="tx"></param>
        /// <returns></returns>
        public int CompactMerge(string tableName, int? metaBlockId, TransactionContext tx)
        {
            var tableMap = Database.GetDatabaseStateSnapshot().TableMap;
            var blocks = LoadBlocks(tableName, metaBlockId, tx)
                .OrderBy(b => b.MinRecordId);

            throw new NotImplementedException();
        }

        private IEnumerable<MetadataBlock> LoadBlocks(
            string tableName,
            int? metaBlockId,
            TransactionContext tx)
        {
            var tableMap = Database.GetDatabaseStateSnapshot().TableMap;
            var metadataTableName = tableMap[tableName].MetadataTableName!;
            var metadataTable = tableMap[metadataTableName].Table;
            var metadataTableSchema = (MetadataTableSchema)metadataTable.Schema;
            var columnIndexes = MetadataBlock.GetColumnIndexes(metadataTableSchema);

            IEnumerable<ReadOnlyMemory<object?>> ReadFromMetaBlock(int metaBlockId)
            {
                var metaMetaBlock = Database.GetOrLoadBlock(metaBlockId, metadataTable.Schema);
                var results = metaMetaBlock.Project(
                    new object?[columnIndexes.Count],
                    columnIndexes.ToImmutableArray(),
                    Enumerable.Range(0, metaMetaBlock.RecordCount),
                    0);

                return results;
            }
            IEnumerable<ReadOnlyMemory<object?>> ReadFromMemoryBlocks()
            {
                var results = metadataTable.Query(tx)
                    //  Especially relevant for availability-block:
                    //  We just want to deal with what is committed
                    .WithCommittedOnly()
                    .WithInMemoryOnly()
                    .WithProjection(columnIndexes);

                return results;
            }

            var results = metaBlockId != null
                ? ReadFromMetaBlock(metaBlockId.Value)
                : ReadFromMemoryBlocks();
            var blocks = results
                .Select(r => MetadataBlock.Create(r.Span))
                .ToImmutableArray();

            return blocks;
        }
    }
}
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib.InMemory.Block;
using TrackDb.Lib.SystemData;

namespace TrackDb.Lib.DataLifeCycle
{
    internal abstract class BlockMergingAgentBase : DataLifeCycleAgentBase
    {
        #region Inner Types
        protected record MetadataRecord(
            string metadataTableName,
            long RecordId,
            int BlockId,
            int Size,
            long MinRecordId,
            ReadOnlyMemory<object?> Record)
        {
            public static IEnumerable<MetadataRecord> LoadMetaRecords(IBlock block)
            {
                var rawRecords = block.Project(
                    new object?[block.TableSchema.Columns.Count + 1],
                    Enumerable.Range(0, block.TableSchema.Columns.Count + 1).ToImmutableArray(),
                    Enumerable.Range(0, block.RecordCount),
                    0);
                var metadataSchemaManager = MetadataSchemaManager.FromMetadataTableSchema(
                    block.TableSchema);
                var records = rawRecords
                    .Select(r => new(
                        block.TableSchema.TableName,
                        (long)r.Span[block.TableSchema.RecordIdColumnIndex]!,
                        (int)r.Span[metadataSchemaManager.BlockIdColumnIndex]!,
                        (int)r.Span[metadataSchemaManager.SizeColumnIndex]!,
                        throw new NotImplementedException(),
                        r.ToArray()));

                return records;
            }
        }
        #endregion

        public BlockMergingAgentBase(
            Database database,
            TypedTable<TombstoneRecord> tombstoneTable,
            Lazy<DatabaseFileManager> storageManager)
            : base(database, tombstoneTable, storageManager)
        {
        }

        public override bool Run(DataManagementActivity forcedDataManagementActivity)
        {
            var doMergeAll =
                (forcedDataManagementActivity & DataManagementActivity.BlockMergeFirstGeneration) != 0;
            var doPersistMetadata =
                (forcedDataManagementActivity & DataManagementActivity.PersistAllMetaDataFirstLevel) != 0;

            return RunMerge(doMergeAll, doPersistMetadata);
        }

        protected abstract bool RunMerge(bool doMergeAll, bool doPersistMetadata);

        protected bool MergeBlocks(
            IEnumerable<MetadataRecord> neighbours,
            MetadataRecord blockToMerge,
            bool doForceHardDelete)
        {
            if (neighbours.Any() || doForceHardDelete)
            {
                throw new NotImplementedException();
            }

            return false;
        }
    }
}
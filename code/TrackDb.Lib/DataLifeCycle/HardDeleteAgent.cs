using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib.Cache.CachedBlock;
using TrackDb.Lib.DbStorage;
using TrackDb.Lib.Predicate;

namespace TrackDb.Lib.DataLifeCycle
{
    internal class HardDeleteAgent : DataLifeCycleAgentBase
    {
        public HardDeleteAgent(
            Database database,
            TypedTable<TombstoneRecord> tombstoneTable,
            Lazy<StorageManager> storageManager)
            : base(database, tombstoneTable, storageManager)
        {
        }

        public override bool Run(DataManagementActivity forcedDataManagementActivity)
        {
            var doHardDeleteAll =
                (forcedDataManagementActivity & DataManagementActivity.HardDeleteAll) != 0;

            return RecordCountBasedHardDelete()
                && AgeBasedHardDelete(doHardDeleteAll);
        }

        #region Record count based
        private bool RecordCountBasedHardDelete()
        {
            for (var candidateTable = FindRecordCountBasedMergeCandidate();
                 candidateTable != null;
                 candidateTable = FindRecordCountBasedMergeCandidate())
            {
                MergeTableTransactionLogs(candidateTable);
            }
            using (var tc = Database.CreateDummyTransaction())
            {
                var tombstoneRecordCount = TombstoneTable.Query(tc).Count();

                if (tombstoneRecordCount > Database.DatabasePolicies.MaxTombstonedRecords)
                {
                    HardDeleteOldest(tc);

                    return false;
                }
                else
                {
                    return true;
                }
            }
        }

        private string? FindRecordCountBasedMergeCandidate()
        {
            using (var tc = Database.CreateDummyTransaction())
            {
                var tombstoneRecordCount = TombstoneTable.Query(tc).Count();

                if (tombstoneRecordCount > Database.DatabasePolicies.MaxTombstonedRecords)
                {
                    var tableGroups = TombstoneTable.Query(tc)
                        .Where(TombstoneTable.PredicateFactory.Equal(t => t.BlockId, null))
                        .GroupBy(t => t.TableName);
                    string? largestTable = null;
                    int maxRecordCount = 0;

                    //  Arg-max
                    foreach (var group in tableGroups)
                    {
                        var recordCount = group.Count();

                        if (recordCount > maxRecordCount)
                        {
                            maxRecordCount = recordCount;
                        }
                        largestTable = group.Key;
                    }
                    if (largestTable != null)
                    {
                        return largestTable;
                    }
                }

                return null;
            }
        }
        #endregion

        #region Age based
        private bool AgeBasedHardDelete(bool doHardDeleteAll)
        {
            using (var tc = Database.CreateDummyTransaction())
            {
                var oldestTombstone = TombstoneTable.Query(tc)
                    .OrderByDesc(t => t.Timestamp)
                    .Take(1)
                    .FirstOrDefault();

                if (oldestTombstone != null)
                {
                    var delta = DateTime.Now.Subtract(oldestTombstone.Timestamp);

                    if (delta > Database.DatabasePolicies.MaxTombstonePeriod
                        || doHardDeleteAll)
                    {
                        HardDeleteOldest(tc);

                        return false;
                    }
                }

                return true;
            }
        }
        #endregion

        #region Hard Delete
        private void HardDeleteOldest(TransactionContext tc)
        {
            var oldestTombstone = TombstoneTable.Query(tc)
                .OrderByDesc(t => t.Timestamp)
                .Take(1)
                .First();
            var blockId = oldestTombstone.BlockId;

            if (blockId == null)
            {   //  The oldest tombstone is from an in-memory block
                MergeTableTransactionLogs(oldestTombstone.TableName);
            }
            else
            {
                HardDeleteBlock(
                    tc,
                    Database.GetAnyTable(oldestTombstone.TableName),
                    blockId.Value);

                return;
            }
        }

        private void HardDeleteBlock(TransactionContext tc, Table table, int blockId)
        {
            var tombstoneRecordIds = TombstoneTable.Query(tc)
                .Where(TombstoneTable.PredicateFactory.Equal(t => t.BlockId, blockId))
                .Select(t => t.RecordId)
                //  In case a record got deleted twice (in 2 parallel transactions)
                .Distinct()
                .ToImmutableArray();
            var metaDataTable = Database.GetMetaDataTable(table.Schema.TableName);
            var metaDataRecordsQuery = new TableQuery(
                metaDataTable,
                tc,
                //  We're looking for the block ID in the meta data table
                new BinaryOperatorPredicate(
                    metaDataTable.Schema.FindColumnIndex(MetadataColumns.BLOCK_ID),
                    blockId,
                    BinaryOperator.Equal),
                Enumerable.Range(0, metaDataTable.Schema.Columns.Count))
                .WithTake(1);
            var metaDataRecords = metaDataRecordsQuery.ToImmutableArray();

            if (metaDataRecords.Any())
            {
                metaDataRecordsQuery.Delete();
                HardDeleteBlock(
                    tc,
                    table,
                    metaDataTable,
                    blockId,
                    metaDataRecords.First(),
                    tombstoneRecordIds);

                return;
            }
            else
            {   //  Block ID is referenced in the tombstone table but is deleted from the meta data
                //  This is because of parallel transactions
                //  We must assume the related record IDs might still be present somewhere
                //  either in-memory or in other blocks
                HardDeleteRogueRecordIds(tc, table, tombstoneRecordIds);
            }
        }

        private void HardDeleteBlock(
            TransactionContext tc,
            Table table,
            Table metaDataTable,
            int blockId,
            ReadOnlyMemory<object?> metaDataRecord,
            IImmutableList<long> tombstoneRecordIds)
        {
            //  1-  Delete tombstone records
            //  2-  Delete metadata row
            //  3-  Load block, transfer to in-memory & delete record
            //  (3 is optimized if all records of the block are deleted)
            var serializedBlockMetadata = SerializedBlockMetaData.FromMetaDataRecord(
                metaDataRecord,
                out var serializedBlockId);
            var blockRecordCount = ((int?)metaDataRecord.Span[
                metaDataTable.Schema.FindColumnIndex(MetadataColumns.ITEM_COUNT)])!.Value;
            var tombstoneBuilder = HardDeleteTombstone(tc, tombstoneRecordIds);

            if (blockId != serializedBlockId)
            {
                throw new InvalidOperationException(
                    $"Block ID mismatch:  expected '{blockId}' " +
                    $"and got '{serializedBlockId}'");
            }
            //  Append the deletion of metadata delete
            tombstoneBuilder.AppendBlock(
                tc.TransactionState.UncommittedTransactionLog.TableBlockBuilderMap[TombstoneTable.Schema.TableName]);
            if (blockRecordCount > tombstoneRecordIds.Count)
            {
                var block = Database.GetOrLoadBlock(
                    blockId,
                    table.Schema,
                    serializedBlockMetadata);
                var blockBuilder = new BlockBuilder(table.Schema);
                var alterTableMap = ImmutableDictionary<string, BlockBuilder>.Empty
                    .Add(TombstoneTable.Schema.TableName, tombstoneBuilder);
                var addTableMap = ImmutableDictionary<string, BlockBuilder>.Empty
                    .Add(table.Schema.TableName, blockBuilder);

                blockBuilder.AppendBlock(block);
                blockBuilder.DeleteRecordsByRecordId(tombstoneRecordIds);
                CommitAlteredLogs(alterTableMap, addTableMap, tc);
            }
            else
            {   //  The block is entirely deleted
                var alterTableMap = ImmutableDictionary<string, BlockBuilder>.Empty
                    .Add(TombstoneTable.Schema.TableName, tombstoneBuilder);
                var addTableMap = ImmutableDictionary<string, BlockBuilder>.Empty;

                CommitAlteredLogs(alterTableMap, addTableMap, tc);
            }
        }

        private BlockBuilder HardDeleteTombstone(
            TransactionContext tc,
            IImmutableList<long> tombstoneRecordIds)
        {
            var tombstoneBuilder = new BlockBuilder(TombstoneTable.Schema);
            var tombstoneColumnCount = TombstoneTable.Schema.Columns.Count;
            var remainingTombstoneRecords = new TableQuery(
                TombstoneTable,
                tc,
                TombstoneTable.PredicateFactory.NotIn(t => t.RecordId, tombstoneRecordIds),
                //  Include record ID
                Enumerable.Range(0, tombstoneColumnCount + 1));

            foreach (var tombstoneRecord in remainingTombstoneRecords)
            {
                tombstoneBuilder.AppendRecord(
                    ((long?)tombstoneRecord.Span[0]!).Value,
                    tombstoneRecord.Slice(0, tombstoneColumnCount).Span);
            }

            return tombstoneBuilder;
        }

        private void HardDeleteRogueRecordIds(
            TransactionContext tc,
            Table table,
            IImmutableList<long> tombstoneRecordIds)
        {
            var newTombstoneRecords = new TableQuery(
                table,
                tc,
                new InPredicate(table.Schema.Columns.Count, tombstoneRecordIds.Cast<object?>()),
                //  Project Record ID + Block ID
                new[] { table.Schema.Columns.Count, table.Schema.Columns.Count + 2 })
                .WithIgnoreDeleted()
                .Select(r => new
                {
                    RecordId = ((long?)r.Span[0])!.Value,
                    BlockId = (int?)r.Span[1]
                })
                .Select(r => new TombstoneRecord(
                    r.RecordId,
                    r.BlockId > 0 ? r.BlockId : null,
                    table.Schema.TableName,
                    DateTime.Now))
                .ToImmutableArray();
            var tombstoneBuilder = HardDeleteTombstone(tc, tombstoneRecordIds);
            var alterTableMap = ImmutableDictionary<string, BlockBuilder>.Empty
                .Add(TombstoneTable.Schema.TableName, tombstoneBuilder);

            if (newTombstoneRecords.Any())
            {   //  Insert new tombstone records in a temp transaction to generate block-builder
                using (var tcTemp = Database.CreateTransaction())
                {
                    foreach (var r in newTombstoneRecords)
                    {
                        TombstoneTable.AppendRecord(r, tcTemp);
                    }

                    var tempBuilder =
                        tcTemp.TransactionState.UncommittedTransactionLog.TableBlockBuilderMap[
                            TombstoneTable.Schema.TableName];

                    tombstoneBuilder.AppendBlock(tempBuilder);
                }
            }
            CommitAlteredLogs(
                alterTableMap,
                ImmutableDictionary<string, BlockBuilder>.Empty,
                tc);
        }
        #endregion
    }
}
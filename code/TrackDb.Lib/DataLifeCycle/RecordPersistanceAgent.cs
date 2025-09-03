using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib.Cache;
using TrackDb.Lib.Cache.CachedBlock;
using TrackDb.Lib.DbStorage;

namespace TrackDb.Lib.DataLifeCycle
{
    internal class RecordPersistanceAgent : DataLifeCycleAgentBase
    {
        public RecordPersistanceAgent(
            Database database,
            TypedTable<TombstoneRecord> tombstoneTable,
            Lazy<StorageManager> storageManager)
            : base(database, tombstoneTable, storageManager)
        {
        }

        public override bool Run(DataManagementActivity forcedDataManagementActivity)
        {
            var doPersistAll =
                (forcedDataManagementActivity & DataManagementActivity.PersistAllData) != 0;

            return PersistOldRecords(doPersistAll);
        }

        private bool PersistOldRecords(bool doPersistEverything)
        {
            using (var tc = Database.CreateDummyTransaction())
            {
                var tableName = GetOldestTable(doPersistEverything, tc);

                if (tableName != null)
                {
                    (var tableBlock, var tombstoneBlock) =
                        MergeTableTransactionLogs(tableName, tc);
                    var metadataTable =
                        Database.GetMetaDataTable(((IBlock)tableBlock).TableSchema.TableName);
                    var metadataBlock = new BlockBuilder(metadataTable.Schema);
                    var isFirstBlockToPersist = true;

                    tableBlock.OrderByRecordId();
                    while (((IBlock)tableBlock).RecordCount > 0)
                    {
                        var blockToPersist = tableBlock.TruncateBlock(StorageManager.BlockSize);
                        var rowCount = ((IBlock)blockToPersist).RecordCount;

                        //  We stop before persisting the last (typically incomplete) block
                        if (isFirstBlockToPersist
                            || ((IBlock)tableBlock).RecordCount == rowCount)
                        {
                            var serializedBlock = blockToPersist.Serialize();
                            var blockId = StorageManager.WriteBlock(serializedBlock.Payload.ToArray());

                            tableBlock.DeleteRecordsByRecordIndex(Enumerable.Range(0, rowCount));
                            metadataBlock.AppendRecord(
                                Database.NewRecordId(),
                                serializedBlock.MetaData.CreateMetaDataRecord(blockId));
                            isFirstBlockToPersist = false;
                        }
                    }
                    CommitPersistance(tableBlock, metadataBlock, tombstoneBlock, tc);
                }

                return tableName != null;
            }
        }

        private void CommitPersistance(
            BlockBuilder tableBlock,
            BlockBuilder metadataBlock,
            BlockBuilder? tombstoneBlock,
            TransactionContext tc)
        {
            var mapBuilder = ImmutableDictionary<string, BlockBuilder>.Empty.ToBuilder();

            mapBuilder.Add(((IBlock)tableBlock).TableSchema.TableName, tableBlock);
            mapBuilder.Add(((IBlock)metadataBlock).TableSchema.TableName, metadataBlock);
            if (tombstoneBlock != null)
            {
                mapBuilder.Add(((IBlock)tombstoneBlock).TableSchema.TableName, tombstoneBlock);
            }

            CommitAlteredLogs(mapBuilder.ToImmutable(), tc);
        }

        private bool ShouldPersistUserData(bool doPersistEverything, TransactionContext tc)
        {
            var tableMap = Database.GetDatabaseStateSnapshot().TableMap;
            var cache = tc.TransactionState.DatabaseCache;
            var totalUserRecords = cache.TableTransactionLogsMap
                .Where(p => tableMap[p.Key].IsUserTable)
                .Select(p => p.Value)
                .Sum(logs => logs.InMemoryBlocks.Sum(b => b.RecordCount));

            return (doPersistEverything && totalUserRecords > 0)
                || totalUserRecords > Database.DatabasePolicies.MaxUnpersistedUserDataRecords;
        }

        private bool ShouldPersistMetaData(bool doPersistEverything, TransactionContext tc)
        {
            var tableMap = Database.GetDatabaseStateSnapshot().TableMap;
            var cache = tc.TransactionState.DatabaseCache;
            var totalMetaDataRecords = cache.TableTransactionLogsMap
                .Where(p => tableMap[p.Key].IsMetaDataTable)
                .Select(p => p.Value)
                .Sum(logs => logs.InMemoryBlocks.Sum(b => b.RecordCount));

            return (doPersistEverything && totalMetaDataRecords > 0)
                || totalMetaDataRecords > Database.DatabasePolicies.MaxUnpersistedMetaDataRecords;
        }

        private string? GetOldestTable(bool doPersistEverything, TransactionContext tc)
        {
            var cache = tc.TransactionState.DatabaseCache;
            var doMetaData = ShouldPersistMetaData(doPersistEverything, tc);
            var doUserData = ShouldPersistUserData(doPersistEverything, tc);

            if (doMetaData || doUserData)
            {
                var oldestRecordId = long.MaxValue;
                var oldestTableName = (string?)null;
                var buffer = new object?[1].AsMemory();
                var rowIndexes = new[] { 0 };
                var projectedColumns = new int[1];
                var tableMap = Database.GetDatabaseStateSnapshot().TableMap;

                foreach (var pair in cache.TableTransactionLogsMap)
                {
                    var tableName = pair.Key;
                    var logs = pair.Value;
                    var table = Database.GetTable(tableName);
                    var isTableElligible = (doMetaData && tableMap[tableName].IsMetaDataTable)
                        || (doUserData && tableMap[tableName].IsUserTable);

                    if (isTableElligible)
                    {
                        var block = logs.InMemoryBlocks
                            .Where(b => b.RecordCount > 0)
                            .FirstOrDefault();

                        if (block != null)
                        {   //  Fetch the record ID
                            projectedColumns[0] = block.TableSchema.Columns.Count;

                            var blockOldestRecordId = block.Project(buffer, projectedColumns, rowIndexes, 0)
                                .Select(r => ((long?)r.Span[0])!.Value)
                                .First();

                            if (blockOldestRecordId < oldestRecordId)
                            {
                                oldestRecordId = blockOldestRecordId;
                                oldestTableName = tableName;
                            }
                        }
                    }
                }

                return oldestTableName;
            }
            else
            {
                return null;
            }
        }
    }
}
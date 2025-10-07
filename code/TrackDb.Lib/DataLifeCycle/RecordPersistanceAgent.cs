using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib.InMemory.Block;
using TrackDb.Lib.DbStorage;
using TrackDb.Lib.SystemData;

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
            var doPersistAllUserData =
                (forcedDataManagementActivity & DataManagementActivity.PersistAllUserData) != 0;
            var doPersistAllMetaData =
                (forcedDataManagementActivity & DataManagementActivity.PersistAllMetaData) != 0;

            return PersistOldRecords(doPersistAllUserData, doPersistAllMetaData);
        }

        private bool PersistOldRecords(bool doPersistAllUserData, bool doPersistAllMetaData)
        {
            using (var tc = Database.CreateDummyTransaction())
            {
                var tableName = GetOldestTable(doPersistAllUserData, doPersistAllMetaData, tc);

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
                        if (isFirstBlockToPersist || ((IBlock)tableBlock).RecordCount > rowCount)
                        {
                            var serializedBlock = blockToPersist.Serialize();
                            var blockId = Database.GetFreeBlockId();
                            
                            StorageManager.WriteBlock(blockId, serializedBlock.Payload.Span);
                            tableBlock.DeleteRecordsByRecordIndex(Enumerable.Range(0, rowCount));
                            metadataBlock.AppendRecord(
                                Database.NewRecordId(),
                                serializedBlock.MetaData.CreateMetaDataRecord(blockId));
                            isFirstBlockToPersist = false;
                        }
                        else
                        {   //  We're done
                            break;
                        }
                    }
                    CommitPersistance(tableBlock, metadataBlock, tombstoneBlock, tc);
                }

                return tableName == null;
            }
        }

        private void CommitPersistance(
            BlockBuilder tableBlock,
            BlockBuilder metadataBlock,
            BlockBuilder? tombstoneBlock,
            TransactionContext tc)
        {
            var replaceMapBuilder = ImmutableDictionary<string, BlockBuilder>.Empty.ToBuilder();
            var addMapBuilder = ImmutableDictionary<string, BlockBuilder>.Empty.ToBuilder();

            replaceMapBuilder.Add(((IBlock)tableBlock).TableSchema.TableName, tableBlock);
            if (tombstoneBlock != null)
            {
                replaceMapBuilder.Add(
                    ((IBlock)tombstoneBlock).TableSchema.TableName,
                    tombstoneBlock);
            }
            addMapBuilder.Add(((IBlock)metadataBlock).TableSchema.TableName, metadataBlock);

            CommitAlteredLogs(
                replaceMapBuilder.ToImmutable(),
                addMapBuilder.ToImmutable(),
                tc);
        }

        private bool ShouldPersistUserData(bool doPersistEverything, TransactionContext tc)
        {
            var tableMap = Database.GetDatabaseStateSnapshot().TableMap;
            var inMemoryDb = tc.TransactionState.InMemoryDatabase;
            var totalUserRecords = inMemoryDb.TableTransactionLogsMap
                .Where(p => tableMap[p.Key].IsUserTable)
                .Select(p => p.Value)
                .Sum(logs => logs.InMemoryBlocks.Sum(b => b.RecordCount));

            return (doPersistEverything && totalUserRecords > 0)
                || totalUserRecords > Database.DatabasePolicies.InMemoryPolicies.MaxUserDataRecords;
        }

        private bool ShouldPersistMetaData(bool doPersistEverything, TransactionContext tc)
        {
            var tableMap = Database.GetDatabaseStateSnapshot().TableMap;
            var inMemoryDb = tc.TransactionState.InMemoryDatabase;
            var totalMetaDataRecords = inMemoryDb.TableTransactionLogsMap
                .Where(p => tableMap[p.Key].IsMetaDataTable)
                .Select(p => p.Value)
                .Sum(logs => logs.InMemoryBlocks.Sum(b => b.RecordCount));

            return (doPersistEverything && totalMetaDataRecords > 0)
                || totalMetaDataRecords > Database.DatabasePolicies.InMemoryPolicies.MaxMetaDataRecords;
        }

        private string? GetOldestTable(
            bool doPersistAllUserData,
            bool doPersistAllMetaData,
            TransactionContext tc)
        {
            var inMemoryDb = tc.TransactionState.InMemoryDatabase;
            var doUserData = ShouldPersistUserData(doPersistAllUserData, tc);
            var doMetaData = ShouldPersistMetaData(doPersistAllMetaData, tc);

            if (doUserData || doMetaData)
            {
                var oldestRecordId = long.MaxValue;
                var oldestTableName = (string?)null;
                var buffer = new object?[1].AsMemory();
                var rowIndexes = new[] { 0 };
                var projectedColumns = new int[1];
                var tableMap = Database.GetDatabaseStateSnapshot().TableMap;

                foreach (var pair in inMemoryDb.TableTransactionLogsMap)
                {
                    var tableName = pair.Key;
                    var logs = pair.Value;
                    var table = Database.GetAnyTable(tableName);
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
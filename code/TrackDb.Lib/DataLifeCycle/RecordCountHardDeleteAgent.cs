using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib.InMemory.Block;

namespace TrackDb.Lib.DataLifeCycle
{
    internal class RecordCountHardDeleteAgent : DataLifeCycleAgentBase
    {
        public RecordCountHardDeleteAgent(Database database)
            : base(database)
        {
        }

        public override void Run(DataManagementActivity forcedDataManagementActivity)
        {
            var doHardDeleteAll =
                (forcedDataManagementActivity & DataManagementActivity.HardDeleteAll)
                == DataManagementActivity.HardDeleteAll;
            var maxTombstonedRecords = Database.DatabasePolicy.InMemoryPolicy.MaxTombstonedRecords;

            while (Iteration(doHardDeleteAll, maxTombstonedRecords))
            {
            }
        }

        private bool Iteration(bool doHardDeleteAll, int maxTombstonedRecords)
        {
            using (var tx = Database.CreateTransaction())
            {
                var result = TransactionalIteration(doHardDeleteAll, maxTombstonedRecords, tx);

                tx.Complete();

                return result;
            }
        }

        private bool TransactionalIteration(
            bool doHardDeleteAll,
            int maxTombstonedRecords,
            TransactionContext tx)
        {
            var doIncludeSystemTables = !doHardDeleteAll;
            var tableMap = Database.GetDatabaseStateSnapshot().TableMap;
            var tombstoneCardinality = Database.TombstoneTable.Query(tx)
                .WithCommittedOnly()
                //  We remove the combination of system table under doHardDeleteAll
                //  As it creates a forever loop with the available-blocks table
                .Where(t => doIncludeSystemTables || !tableMap[t.TableName].IsSystemTable)
                .Count();

            if ((doHardDeleteAll && tombstoneCardinality > 0)
                || tombstoneCardinality > maxTombstonedRecords)
            {
                CleanOneBlock(doIncludeSystemTables, tx);

                return true;
            }
            else
            {
                return false;
            }
        }

        private void CleanOneBlock(bool doIncludeSystemTables, TransactionContext tx)
        {
            var tableMap = Database.GetDatabaseStateSnapshot().TableMap;
            var topBlocks = Database.TombstoneTable.Query(tx)
                .WithCommittedOnly()
                .Where(t => doIncludeSystemTables || !tableMap[t.TableName].IsSystemTable)
                //  Count records per block
                .CountBy(t => (t.TableName, t.BlockId ?? 0))
                .Select(p => new
                {
                    p.Key.TableName,
                    BlockId = p.Key.Item2 <= 0 ? null : (int?)p.Key.Item2,
                    RecordCount = p.Value
                })
                .OrderByDescending(o => o.RecordCount)
                //  Cap the collection to required item count
                .Take(2 * Database.DatabasePolicy.InMemoryPolicy.MaxNonMetaDataRecords);
            var tableName = topBlocks.First().TableName;
            var hasNullBlocks = topBlocks
                .Where(o => o.TableName == tableName)
                .Where(o => o.BlockId == null)
                .Any();

            if (tableMap[tableName].IsMetaDataTable)
            {
                throw new InvalidOperationException(
                    $"A metadata table ({tableName}) has tombstone entries");
            }
            if (hasNullBlocks)
            {
                var tombstoneBlockFixLogic = new TombstoneBlockFixLogic(Database);

                tombstoneBlockFixLogic.FixNullBlockIds(tableName, tx);
            }
            else
            {
                var blockId = topBlocks.First().BlockId!.Value;
                var otherBlockIds = topBlocks
                    .Skip(1)
                    .Where(o => o.TableName == tableName)
                    .Select(o => o.BlockId!.Value)
                    .ToImmutableArray();

                CompactBlock(tableName, blockId, otherBlockIds, tx);
            }
        }

        private void CompactBlock(
            string tableName,
            int blockId,
            IEnumerable<int> otherBlockIds,
            TransactionContext tx)
        {
            var blockMergingLogic = new BlockMergingLogic(Database);

            if (!blockMergingLogic.CompactBlock(tableName, blockId, otherBlockIds, tx))
            {
                var tombstoneBlockFixLogic = new TombstoneBlockFixLogic(Database);

                tombstoneBlockFixLogic.FixBlockId(tableName, blockId, tx);
            }
        }
    }
}
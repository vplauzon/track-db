using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib.DataLifeCycle
{
    internal class RecordCountHardDeleteAgent : DataLifeCycleAgentBase
    {
        public RecordCountHardDeleteAgent(Database database)
            : base(database)
        {
        }

        public override void Run(
            DataManagementActivity forcedDataManagementActivity,
            TransactionContext tx)
        {
            var doHardDeleteAll =
                (forcedDataManagementActivity & DataManagementActivity.HardDeleteAll)
                == DataManagementActivity.HardDeleteAll;
            var maxTombstonedRecords = Database.DatabasePolicy.InMemoryPolicy.MaxTombstonedRecords;

            while (Iteration(doHardDeleteAll, maxTombstonedRecords, tx))
            {
            }
        }

        private bool Iteration(
            bool doHardDeleteAll,
            int maxTombstonedRecords,
            TransactionContext tx)
        {
            var tombstoneCardinality = Database.TombstoneTable.Query(tx)
                .Count();

            if (doHardDeleteAll && tombstoneCardinality > 0)
            {
                FindBlock((int)tombstoneCardinality, tx);

                return true;
            }
            else if (tombstoneCardinality > maxTombstonedRecords)
            {
                FindBlock((int)(tombstoneCardinality - maxTombstonedRecords), tx);

                return true;
            }
            else
            {
                return false;
            }
        }

        private void FindBlock(int recordCountToHardDelete, TransactionContext tx)
        {
            var topBlocks = Database.TombstoneTable.Query(tx)
                //  Count records per block
                .CountBy(t => (t.TableName, t.BlockId ?? 0))
                .Select(p => new
                {
                    p.Key.TableName,
                    BlockId = p.Key.Item2,
                    RecordCount = p.Value
                })
                .OrderBy(o => o.RecordCount)
                //  Cap the collection to required item count
                .CapSumValues(o => o.RecordCount, recordCountToHardDelete);
            var tableName = topBlocks.First().TableName;
            var hasNullBlocks = topBlocks
                .Where(o => o.TableName == tableName)
                .Where(o => o.BlockId == 0)
                .Any();
            var blockId = topBlocks.First().BlockId;
            var otherBlockIds = topBlocks
                .Skip(1)
                .Where(o => o.TableName == tableName)
                .Select(o => o.BlockId)
                .ToImmutableArray();

            //  GC
            topBlocks = topBlocks.Take(0).ToImmutableArray();
            if (hasNullBlocks)
            {
                var tombstoneBlockFixLogic = new TombstoneBlockFixLogic(Database);

                tombstoneBlockFixLogic.FixNullBlockIds(tableName, tx);
            }
            else
            {
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

            if (blockMergingLogic.CompactBlock(tableName, blockId, otherBlockIds, tx))
            {
                throw new NotImplementedException();
            }
        }
    }
}
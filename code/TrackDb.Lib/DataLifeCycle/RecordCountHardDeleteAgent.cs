using Azure.Storage.Blobs.Models;
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

            while (HardDeleteIteration(doHardDeleteAll, maxTombstonedRecords, tx))
            {
            }
        }

        private bool HardDeleteIteration(
            bool doHardDeleteAll,
            int maxTombstonedRecords,
            TransactionContext tx)
        {
            var tombstoneCardinality = Database.TombstoneTable.Query(tx)
                .Count();

            if ((doHardDeleteAll && tombstoneCardinality > 0)
                || tombstoneCardinality > maxTombstonedRecords)
            {
                HardDeleteByTable((int)(tombstoneCardinality - maxTombstonedRecords), tx);

                return true;
            }
            else
            {
                return false;
            }
        }

        private void HardDeleteByTable(int recordCountToHardDelete, TransactionContext tx)
        {
            bool FixNullBlockIds(string tableName, TransactionContext tx)
            {
                throw new NotImplementedException();
            }

            var argMaxTableName = Database.TombstoneTable.Query(tx)
                .CountBy(t => t.TableName)
                .MaxBy(p => p.Value)
                .Key;
            var isNewlyLoaded = tx.LoadCommittedBlocksInTransaction(argMaxTableName);
            var hasNullBlockIds = FixNullBlockIds(argMaxTableName, tx);

            if (!isNewlyLoaded && !hasNullBlockIds)
            {
                HardDeleteByBlock(argMaxTableName, tx);
            }
        }

        private void HardDeleteByBlock(string tableName, TransactionContext tx)
        {
            var argMaxBlockId = Database.TombstoneTable.Query(tx)
                .Where(pf => pf.Equal(t => t.TableName, tableName))
                .CountBy(t => t.BlockId!.Value)
                .MaxBy(p => p.Key)
                .Key;
            var otherBlockIds = Database.TombstoneTable.Query(tx)
                .Where(pf => pf.Equal(t => t.TableName, tableName))
                .Where(pf => pf.NotEqual(t => t.BlockId, argMaxBlockId))
                .Select(t => t.BlockId!.Value)
                .Distinct()
                .ToImmutableArray();
            var blockMergingLogic = new BlockMergingLogic(Database);

            if (!blockMergingLogic.Compact(tableName, argMaxBlockId, otherBlockIds, tx))
            {
                throw new NotImplementedException();
            }
        }
    }
}
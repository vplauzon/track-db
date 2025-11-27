using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib.DataLifeCycle
{
    internal class TimeHardDeleteAgent : DataLifeCycleAgentBase
    {
        public TimeHardDeleteAgent(Database database)
            : base(database)
        {
        }

        public override void Run(
            DataManagementActivity forcedDataManagementActivity,
            TransactionContext tx)
        {
            while (HardDeleteIteration(tx))
            {
            }
        }

        private bool HardDeleteIteration(TransactionContext tx)
        {
            bool FixNullBlockIds(string tableName, TransactionContext tx)
            {
                throw new NotImplementedException();
            }

            var thresholdTimestamp = DateTime.Now
                - Database.DatabasePolicy.InMemoryPolicy.MaxTombstonePeriod;
            var oldTombstoneRecords = Database.TombstoneTable.Query(tx)
                .Where(pf => pf.LessThan(t => t.Timestamp, thresholdTimestamp));
            var tombstoneCount = oldTombstoneRecords.Count();

            if (tombstoneCount > 0)
            {
                var oldestRecord = oldTombstoneRecords
                    .MaxBy(t => t.Timestamp);
                var argMaxTableName = oldestRecord!.TableName;
                var argMaxBlockId = oldestRecord!.BlockId!.Value;
                var isNewlyLoaded = tx.LoadCommittedBlocksInTransaction(argMaxTableName);
                var hasNullBlockIds = FixNullBlockIds(argMaxTableName, tx);

                if (!isNewlyLoaded && !hasNullBlockIds)
                {
                    var otherBlockIds = oldTombstoneRecords
                        .Where(pf => pf.Equal(t => t.TableName, argMaxTableName))
                        .Where(pf => pf.NotEqual(t => t.BlockId, argMaxBlockId))
                        .Select(t => t.BlockId!.Value)
                        .Distinct()
                        .ToImmutableArray();
                    var blockMergingLogic = new BlockMergingLogic(Database);

                    if (!blockMergingLogic.Compact(
                        argMaxTableName,
                        argMaxBlockId,
                        otherBlockIds,
                        tx))
                    {
                        throw new NotImplementedException();
                    }
                }

                return true;
            }
            else
            {
                return false;
            }
        }
    }
}
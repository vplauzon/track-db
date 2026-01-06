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
            var thresholdTimestamp = DateTime.Now
                - Database.DatabasePolicy.InMemoryPolicy.MaxTombstonePeriod;
            var oldTombstoneRecords = Database.TombstoneTable.Query(tx)
                .WherePredicate(pf => pf.LessThan(t => t.Timestamp, thresholdTimestamp));

            if (oldTombstoneRecords.Any())
            {
                var oldestRecord = oldTombstoneRecords
                    .OrderBy(t => t.Timestamp)
                    .Take(1)
                    .First();
                var argMaxTableName = oldestRecord!.TableName;
                var argMaxBlockId = oldestRecord!.BlockId;
                var hasNullBlockIds = oldTombstoneRecords
                    .WherePredicate(pf => pf.Equal(t => t.TableName, argMaxTableName))
                    .WherePredicate(pf => pf.Equal(t => t.BlockId, null))
                    .Any();

                if (argMaxBlockId == null || hasNullBlockIds)
                {
                    var tombstoneBlockFixLogic = new TombstoneBlockFixLogic(Database);

                    tombstoneBlockFixLogic.FixNullBlockIds(argMaxTableName, tx);
                }
                else
                {
                    var otherBlockIds = oldTombstoneRecords
                        .WherePredicate(pf => pf.Equal(t => t.TableName, argMaxTableName))
                        .WherePredicate(pf => pf.NotEqual(t => t.BlockId, argMaxBlockId))
                        .Select(t => t.BlockId!.Value)
                        .Distinct()
                        .ToImmutableArray();
                    var blockMergingLogic = new BlockMergingLogic(Database);

                    if (!blockMergingLogic.CompactBlock(
                        argMaxTableName,
                        argMaxBlockId.Value,
                        otherBlockIds,
                        tx))
                    {
                        var tombstoneBlockFixLogic = new TombstoneBlockFixLogic(Database);

                        tombstoneBlockFixLogic.FixBlockId(argMaxTableName, argMaxBlockId.Value, tx);
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
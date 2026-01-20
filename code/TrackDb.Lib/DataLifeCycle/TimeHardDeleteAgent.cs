using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib.DataLifeCycle.Persistance;

namespace TrackDb.Lib.DataLifeCycle
{
    internal class TimeHardDeleteAgent : DataLifeCycleAgentBase
    {
        public TimeHardDeleteAgent(Database database)
            : base(database)
        {
        }

        public override void Run(DataManagementActivity forcedDataManagementActivity)
        {
            while (HardDeleteIteration())
            {
            }
        }

        private bool HardDeleteIteration()
        {
            using (var tx = Database.CreateTransaction())
            {
                var result = HardDeleteTransactionalIteration(tx);

                tx.Complete();

                return result;
            }
        }

        private bool HardDeleteTransactionalIteration(TransactionContext tx)
        {
            var thresholdTimestamp = DateTime.Now
                - Database.DatabasePolicy.InMemoryPolicy.MaxTombstonePeriod;
            var oldTombstoneRecords = Database.TombstoneTable.Query(tx)
                .WithCommittedOnly()
                .Where(pf => pf.LessThan(t => t.Timestamp, thresholdTimestamp));

            if (oldTombstoneRecords.Any())
            {
                var oldestRecord = oldTombstoneRecords
                    .OrderBy(t => t.Timestamp)
                    .Take(1)
                    .First();
                var argMaxTableName = oldestRecord!.TableName;
                var argMaxBlockId = oldestRecord!.BlockId;
                var hasNullBlockIds = oldTombstoneRecords
                    .Where(pf => pf.Equal(t => t.TableName, argMaxTableName))
                    .Where(pf => pf.Equal(t => t.BlockId, null))
                    .Any();

                if (argMaxBlockId == null || hasNullBlockIds)
                {
                    var tombstoneBlockFixLogic = new TombstoneBlockFixLogic(Database);

                    tombstoneBlockFixLogic.FixNullBlockIds(argMaxTableName, tx);
                }
                else
                {
                    var otherBlockIds = oldTombstoneRecords
                        .Where(pf => pf.Equal(t => t.TableName, argMaxTableName))
                        .Where(pf => pf.NotEqual(t => t.BlockId, argMaxBlockId))
                        .Select(t => t.BlockId!.Value)
                        .Distinct()
                        .ToImmutableArray();
                    var blockMergingLogic = new BlockMergingLogic2(
                        Database,
                        new MetaBlockManager(Database, tx));

                    if (!blockMergingLogic.CompactMerge(argMaxTableName, argMaxBlockId.Value))
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
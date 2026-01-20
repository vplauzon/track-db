using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib.DataLifeCycle.Persistance;
using TrackDb.Lib.Predicate;

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
            bool HardDeleteIteration()
            {
                using (var tx = Database.CreateTransaction())
                {
                    var hasHardDeleted = HardDeleteTransactionalIteration(tx);

                    tx.Complete();

                    return hasHardDeleted;
                }
            }

            while (HardDeleteIteration())
            {
            }
        }

        private bool HardDeleteTransactionalIteration(TransactionContext tx)
        {
            var thresholdTimestamp = DateTime.Now
                - Database.DatabasePolicy.InMemoryPolicy.MaxTombstonePeriod;
            var oldestTombstoneRecord = Database.TombstoneTable.Query(tx)
                .WithCommittedOnly()
                .Where(pf => pf.LessThan(t => t.Timestamp, thresholdTimestamp))
                .FirstOrDefault();

            if (oldestTombstoneRecord != null)
            {
                if (tx.LoadCommittedBlocksInTransaction(oldestTombstoneRecord.TableName))
                {   //  Let's re-evaluate the oldest tombstone
                    return HardDeleteTransactionalIteration(tx);
                }
                else
                {
                    CompactForRecord(
                        oldestTombstoneRecord.TableName,
                        oldestTombstoneRecord.DeletedRecordId,
                        tx);
                    CheckRecordHardDeleted(
                        oldestTombstoneRecord.TableName,
                        oldestTombstoneRecord.DeletedRecordId,
                        tx);

                    return true;
                }
            }
            else
            {
                return false;
            }
        }

        [Conditional("DEBUG")]
        private void CheckRecordHardDeleted(
            string tableName,
            long deletedRecordId,
            TransactionContext tx)
        {
            var recordCount = Database.TombstoneTable.Query(tx)
                .WithCommittedOnly()
                .Where(pf => pf.Equal(t => t.DeletedRecordId, deletedRecordId))
                .Count();

            if (recordCount != 0)
            {
                throw new InvalidOperationException("Record wasn't hard deleted");
            }
        }

        private void CompactForRecord(string tableName, long recordId, TransactionContext tx)
        {
            var table = Database.GetAnyTable(tableName);
            var schema = table.Schema;
            var predicate = new BinaryOperatorPredicate(
                schema.RecordIdColumnIndex,
                recordId,
                BinaryOperator.Equal);
            var blockIds = table.Query(tx)
                .WithCommittedOnly()
                .WithIgnoreDeleted()
                .WithPredicate(predicate)
                .WithProjection(schema.ParentBlockIdColumnIndex)
                .Select(r => (int)r.Span[0]!)
                .ToImmutableArray();

            if (blockIds.Length > 1)
            {
                throw new InvalidOperationException("A record is duplicated");
            }
            else if (blockIds.Length == 1)
            {   //  We identified the block
                if (blockIds[0] <= 0)
                {
                    throw new InvalidOperationException(
                        "Record is in-memory, should have been taken care of before");
                }
                else
                {   //  Let's find the meta-block
                    CompactBlock(table, blockIds[0], tx);
                }
            }
            else
            {   //  Can't find record:  likely due to racing condition
                //  We hard-delete the record
                Database.DeleteTombstoneRecords(tableName, [recordId], tx);
            }
        }

        private void CompactBlock(Table table, int blockId, TransactionContext tx)
        {
            var metaTable = Database.GetMetaDataTable(table.Schema.TableName);
            var metaSchema = (MetadataTableSchema)metaTable.Schema;
            var predicate = new BinaryOperatorPredicate(
                metaSchema.BlockIdColumnIndex,
                blockId,
                BinaryOperator.Equal);
            var metaBlockIds = metaTable.Query(tx)
                .WithCommittedOnly()
                .WithPredicate(predicate)
                .WithProjection(metaSchema.ParentBlockIdColumnIndex)
                .Select(r => (int)r.Span[0]!)
                .ToImmutableArray();

            if (metaBlockIds.Length > 1)
            {
                throw new InvalidOperationException("A record is duplicated");
            }
            else if (metaBlockIds.Length == 0)
            {
                throw new InvalidOperationException("Can't find block");
            }
            else
            {   //  We identified the meta block
                var metaBlockId = metaBlockIds[0];

                CompactMetaBlock(
                    table.Schema.TableName,
                    metaBlockId <= 0
                    ? null
                    : metaBlockId,
                    tx);
            }
        }

        private void CompactMetaBlock(string tableName, int? metaBlockId, TransactionContext tx)
        {
            var blockMergingLogic = new BlockMergingLogic(
               Database,
               new MetaBlockManager(Database, tx));

            blockMergingLogic.CompactMerge(tableName, metaBlockId);
        }
    }
}
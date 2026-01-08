using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using TrackDb.Lib.Predicate;
using TrackDb.Lib.SystemData;

namespace TrackDb.Lib.DataLifeCycle
{
    internal class TombstoneBlockFixLogic : LogicBase
    {
        public TombstoneBlockFixLogic(Database database)
            : base(database)
        {
        }

        /// <summary>Fix <c>null</c> block ids in tombstone records.</summary>
        /// <param name="tableName"></param>
        /// <param name="tx"></param>
        public void FixNullBlockIds(string tableName, TransactionContext tx)
        {   //  First eliminate the nulls being from in-memory blocks by
            //  merging in-memory blocks together
            tx.LoadCommittedBlocksInTransaction(tableName);

            var orphansDeletedRecordMap = Database.TombstoneTable.Query(tx)
                .WithCommittedOnly()
                .Where(pf => pf.Equal(t => t.TableName, tableName))
                .Where(pf => pf.Equal(t => t.BlockId, null).Or(pf.LessThanOrEqual(t => t.BlockId, 0)))
                .GroupBy(t => t.DeletedRecordId)
                //  It's possible to have twice the same record ID in the tombstone with parallel TX
                .ToImmutableDictionary(g => g.Key, g => g.Min(t => t.Timestamp));

            if (orphansDeletedRecordMap.Any())
            {
                var table = Database.GetAnyTable(tableName);
                var predicate = new InPredicate(
                    table.Schema.RecordIdColumnIndex,
                    orphansDeletedRecordMap.Keys.Cast<object?>());
                var foundRecords = table.Query(tx)
                    .WithCommittedOnly()
                    .WithIgnoreDeleted()
                    .WithPredicate(predicate)
                    .WithProjection([
                        table.Schema.RecordIdColumnIndex,
                        table.Schema.ParentBlockIdColumnIndex])
                    .Select(r => new TombstoneRecord(
                        (long)r.Span[0]!,
                        tableName,
                        (int)r.Span[1]!,
                        orphansDeletedRecordMap[(long)r.Span[0]!]))
                    .ToImmutableArray();

                //  We can't assert foundRecords.Count() == orphansDeletedRecordMap.Count()
                //  Parallel transaction could break that assertion
                Database.DeleteTombstoneRecords(
                    tableName,
                    orphansDeletedRecordMap.Keys.Distinct(),
                    false,
                    tx);
                //  This was deleted from committed logs and can hence be hard-deleted there
                Database.TombstoneTable.AppendRecords(foundRecords, tx);
            }
        }

        /// <summary>
        /// Fix block ID in tombstone which isn't valid anymore by searching for the
        /// record IDs directly.
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="blockId"></param>
        /// <param name="tx"></param>
        /// <exception cref="NotImplementedException"></exception>
        public void FixBlockId(string tableName, int blockId, TransactionContext tx)
        {   //  Load table and tombstone table in-memory so we can delete in-place
            tx.LoadCommittedBlocksInTransaction(tableName);
            tx.LoadCommittedBlocksInTransaction(Database.TombstoneTable.Schema.TableName);

            var orphansDeletedRecordIdMap = Database.TombstoneTable.Query(tx)
                .WithCommittedOnly()
                .Where(pf => pf.Equal(t => t.TableName, tableName))
                .Where(pf => pf.Equal(t => t.BlockId, blockId))
                .ToImmutableDictionary(t => t.DeletedRecordId, t => t.Timestamp);
            var table = Database.GetAnyTable(tableName);
            var predicate = new InPredicate(
                table.Schema.RecordIdColumnIndex,
                orphansDeletedRecordIdMap.Keys.Cast<object?>());
            var projectionColumnIndexes = new[]
            {
                table.Schema.RecordIdColumnIndex,
                table.Schema.ParentBlockIdColumnIndex
            };
            var newTombstoneRecords = table.Query(tx)
                .WithIgnoreDeleted()
                .WithPredicate(predicate)
                .WithProjection(projectionColumnIndexes)
                .Select(r => new TombstoneRecord(
                    (long)r.Span[0]!,
                    tableName,
                    (int)r.Span[1]!,
                    orphansDeletedRecordIdMap[(long)r.Span[0]!]));
            //  The delete was on "committed only", the fixed version should be there too
            var tombstoneCommittedDataBlock = tx.TransactionState.UncommittedTransactionLog
                .TransactionTableLogMap[Database.TombstoneTable.Schema.TableName]
                .CommittedDataBlock!;

            //  Delete those records in tombstone table
            Database.DeleteTombstoneRecords(tableName, orphansDeletedRecordIdMap.Keys, false, tx);
            foreach (var record in newTombstoneRecords)
            {
                var columnRecord = Database.TombstoneTable.Schema.FromObjectToColumns(record);

                tombstoneCommittedDataBlock.AppendRecord(
                    record.Timestamp,
                    Database.TombstoneTable.NewRecordId(),
                    columnRecord);
            }
        }
    }
}
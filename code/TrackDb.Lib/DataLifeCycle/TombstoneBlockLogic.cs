using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
using System.Linq;
using System.Text;
using TrackDb.Lib.Predicate;

namespace TrackDb.Lib.DataLifeCycle
{
    internal class TombstoneBlockLogic : LogicBase
    {
        public TombstoneBlockLogic(Database database)
            : base(database)
        {
        }

        /// <summary>
        /// Retrieves tombstone records grouped by table and by blocks.
        /// </summary>
        /// <remarks>
        /// All tables should be loaded in the transaction, i.e. no deleted record
        /// should be in memory.
        /// </remarks>
        /// <param name="tableNames">
        /// If <c>null</c>, all persisting tables are considered.
        /// </param>
        /// <param name="tx"></param>
        /// <returns></returns>
        public IDictionary<string, IEnumerable<TombstoneBlock>> GetTombstoneBlocksMap(
            IEnumerable<string>? tableNames,
            TransactionContext tx)
        {
            var tombstoneSchema = Database.TombstoneTable.Schema;
            var tableMap = Database.GetDatabaseStateSnapshot().TableMap;
            var columnIndexes = tombstoneSchema.GetColumnIndexSubset(t => t.TableName)
                .Concat(tombstoneSchema.GetColumnIndexSubset(t => t.DeletedRecordId));
            var tombstonesByTable = Database.TombstoneTable.Query(tx)
                .TableQuery
                .WithProjection(columnIndexes)
                .Select(r => new
                {
                    TableName = (string)r.Span[0]!,
                    DeletedRecordId = (long)r.Span[1]!
                })
                .GroupBy(o => o.TableName, o => o.DeletedRecordId);
            var tableFilter = tableNames == null
                ? tombstonesByTable.Where(g => tableMap[g.Key].IsPersisted)
                : tombstonesByTable.Where(g => tableNames.Contains(g.Key));
            var tombstoneBlocksMap = tableFilter
                .ToDictionary(g => g.Key, g => GetTombstoneBlocks(g.Key, g, tx));

            return tombstoneBlocksMap;
        }

        private IEnumerable<TombstoneBlock> GetTombstoneBlocks(
            string tableName,
            IEnumerable<long> deletedRecordIds,
            TransactionContext tx)
        {
            var table = Database.GetAnyTable(tableName);
#if DEBUG
            if(table.Schema.IsMetadata)
            {
                throw new NotSupportedException("Metadata tables don't have tombstones");
            }
#endif
            var dataSchema = (DataTableSchema)table.Schema;
            var predicate = new InPredicate<long>(
                dataSchema.RecordIdColumnIndex,
                deletedRecordIds,
                true);
            var blockTraceResults = table.Query(tx)
                .WithIgnoreDeleted()
                .WithPredicate(predicate)
                .WithProjection(dataSchema.RecordIdColumnIndex)
                .ExecuteQueryWithBlockTrace();
            var tombstoneBlockMap = new Dictionary<int, TombstoneBlock>();
            var foundRecordIdSet = new HashSet<long>();

            //  We do not do it in a LINQ query for efficiency
            //  Specifically we want to "ToArray" the block traces only once
            foreach (var blockTraceResult in blockTraceResults)
            {
                var lastBlockTrace = blockTraceResult.BlockTraces.Last();
                var blockId = lastBlockTrace.BlockId;
                var recordId = (long)blockTraceResult.Result.Span[0]!;

                if (!tombstoneBlockMap.TryGetValue(blockId, out var block))
                {
                    block = new TombstoneBlock(
                        blockTraceResult.BlockTraces.SkipLast(1).ToArray(),
                        lastBlockTrace.Schema,
                        blockId,
                        new List<int>(),
                        new List<long>());
                    tombstoneBlockMap[blockId] = block;
                }
                //  Safe cast since this is allocated in the else-branch
                ((IList<int>)block.RowIndexes).Add(lastBlockTrace.RowIndex);
                ((IList<long>)block.RecordIds).Add(recordId);
                foundRecordIdSet.Add(recordId);
            }
            //  Detect and hard-delete phantom records
            var phantomRecordIds = deletedRecordIds
                .Where(id => !foundRecordIdSet.Contains(id))
                .ToArray();

            if (phantomRecordIds.Length > 0)
            {
                Database.DeleteTombstoneRecords(tableName, phantomRecordIds, tx);
            }

            return tombstoneBlockMap.Values;
        }
    }
}
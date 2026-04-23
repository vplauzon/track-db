using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection.Metadata;
using System.Text;
using TrackDb.Lib.DataLifeCycle.Persistance;
using TrackDb.Lib.InMemory.Block;
using TrackDb.Lib.Predicate;

namespace TrackDb.Lib.DataLifeCycle
{
    /// <summary>Logic compacting blocks and merging them together.</summary>
    internal class BlockMergingLogic4 : LogicBase
    {
        #region Inner Types
        private record struct BlockTraceSlim(TableSchema Schema, int BlockId);
        #endregion

        public BlockMergingLogic4(Database database)
            : base(database)
        {
        }

        /// <summary>
        /// Compact multiple block IDs (<paramref name="blockIdsToCompact"/>)
        /// in a single table (<paramref name="tableName"/>).
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="blockIdsToCompact"></param>
        /// <param name="tx"></param>
        public void CompactMerge(
            string tableName,
            IEnumerable<int> blockIdsToCompact,
            TransactionContext tx)
        {
            var blockIdsToCompactSet = blockIdsToCompact.ToFrozenSet();
            var blockTracesToCompact = FetchBlockTraces(tableName, blockIdsToCompactSet, tx);
            //  Each of those are independant as the root is at different level
            var blockTracesGroups = blockTracesToCompact
                .GroupBy(t => t.Count);
            var blockReplacementMap = new Dictionary<int, IEnumerable<MetadataBlock>>();
            var cumulatedDeletedBlockIds = new List<int>();
            var metaBlockMergingLogic = new MetaBlockMergingLogic(Database);

            foreach (var rootGroup in blockTracesGroups)
            {
                var traceLength = rootGroup.Key;

                for (var i = traceLength - 2; i >= 0; --i)
                {
                    var blockTracesByMetaBlockId = rootGroup
                        .GroupBy(t => t[i].BlockId)
                        .Distinct();

                    foreach (var metaBlockGroup in blockTracesByMetaBlockId)
                    {
                        var metaBlockId =
                            metaBlockGroup.Key <= 0 ? (int?)null : metaBlockGroup.Key;
                        var compactResult = metaBlockMergingLogic.CompactMerge(
                            metaBlockId,
                            (MetadataTableSchema)metaBlockGroup.First()[i].Schema,
                            i == traceLength - 2
                            ? metaBlockGroup.Select(o => o[traceLength - 1].BlockId)
                            : Array.Empty<int>(),
                            blockReplacementMap,
                            tx);

                        cumulatedDeletedBlockIds.AddRange(compactResult.DeletedBlockIds);
                        if (metaBlockId != null)
                        {
                            blockReplacementMap.Add(metaBlockId.Value, compactResult.MetaBlocks);
                            cumulatedDeletedBlockIds.Add(metaBlockId.Value);
                        }
                    }
                }
                blockReplacementMap.Clear();
            }
            CleanHardDeletedBlockIds(cumulatedDeletedBlockIds, tx);
        }

        private void CleanHardDeletedBlockIds(List<int> deletedBlockIds, TransactionContext tx)
        {
            var blockTombstonesIndex =
                tx.TransactionState.UncommittedTransactionLog.ReplacingBlockTombstonesIndex!;

            foreach (var id in deletedBlockIds)
            {
                blockTombstonesIndex.Remove(id);
            }
            Database.AvailabilityBlockManager.SetNoLongerInUse(deletedBlockIds, tx);
        }

        private IEnumerable<IReadOnlyList<BlockTraceSlim>> FetchBlockTraces(
            string tableName,
            ISet<int> blockIdsToCompact,
            TransactionContext tx)
        {
            var metaTable = Database.GetMetaDataTable(tableName);
            var metaSchema = (MetadataTableSchema)metaTable.Schema;

            IReadOnlyList<BlockTraceSlim> CreateTrace(BlockTracedResult result)
            {
                return result.BlockTraces
                    .Select(bt => new BlockTraceSlim(bt.Schema, bt.BlockId))
                    .Append(new BlockTraceSlim(metaSchema, (int)result.Result.Span[0]!))
                    .ToArray();
            }

            var predicate = new InPredicate<int>(
                metaSchema.BlockIdColumnIndex,
                blockIdsToCompact,
                true);
            var results = metaTable.Query(tx)
                .WithPredicate(predicate)
                .WithProjection(metaSchema.BlockIdColumnIndex)
                .ExecuteQueryWithBlockTrace()
                .Select(r => CreateTrace(r));

            return results;
        }
    }
}
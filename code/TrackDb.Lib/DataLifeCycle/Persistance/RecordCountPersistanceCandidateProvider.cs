using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TrackDb.Lib.DataLifeCycle.Persistance
{
    internal class RecordCountPersistanceCandidateProvider : LogicBase, IPersistanceCandidateProvider
    {
        #region Inner types
        private record TableRecordCount(Table Table, long RecordCount, bool IsMetaMerged);
        #endregion

        private readonly ITableProvider _tableProvider;

        public RecordCountPersistanceCandidateProvider(
            Database database,
            ITableProvider tableProvider)
            : base(database)
        {
            _tableProvider = tableProvider;
        }

        IEnumerable<PersistanceCandidate> IPersistanceCandidateProvider.FindCandidates(
            DataManagementActivity activity,
            TransactionContext tx)
        {
            var doPersistAll = _tableProvider.DoPersistAll(activity);
            var tableRecordCounts = GetTableRecordCounts(tx);

            while (IsPersistanceRequired(tableRecordCounts, doPersistAll, tx))
            {
                var topCandidate = tableRecordCounts.First();

                tableRecordCounts.RemoveAt(0);
                tx.LoadCommittedBlocksInTransaction(topCandidate.Table.Schema.TableName);

                //  Validate a log merge didn't occur which change the counts
                var newRecordCount = GetRecordCount(topCandidate.Table, tx);

                if (newRecordCount == topCandidate.RecordCount)
                {
                    var tableMap = Database.GetDatabaseStateSnapshot().TableMap;
                    var isMetadataTable =
                        tableMap[topCandidate.Table.Schema.TableName].IsMetaDataTable;

                    if (!isMetadataTable
                        || topCandidate.IsMetaMerged
                        || !MergeMetaRecords(topCandidate, tx))
                    {
                        yield return new PersistanceCandidate(topCandidate.Table, doPersistAll);
                    }
                    else
                    {
                        var newNewRecordCount = GetRecordCount(topCandidate.Table, tx);

                        //  We filter out tables with only one record since storing the meta record
                        //  in memory is equivalent to storing the record itself
                        if (newNewRecordCount > 1)
                        {
                            tableRecordCounts.Add(
                                new TableRecordCount(topCandidate.Table, newNewRecordCount, true));
                            Sort(tableRecordCounts);
                        }
                    }
                }
                else
                {
                    tableRecordCounts.Add(
                        new TableRecordCount(topCandidate.Table, newRecordCount, false));
                    Sort(tableRecordCounts);
                }
            }
        }

        private void Sort(List<TableRecordCount> tableRecordCounts)
        {
            tableRecordCounts.Sort((trc1, trc2) => -trc1.RecordCount.CompareTo(trc2.RecordCount));
        }

        private bool IsPersistanceRequired(
            IEnumerable<TableRecordCount> tableRecordCounts,
            bool doPersistAll,
            TransactionContext tx)
        {
            var totalRecords = tableRecordCounts
                .Sum(trc => trc.RecordCount);

            return totalRecords > _tableProvider.MaxInMemoryDataRecords
                || (totalRecords > 0 && doPersistAll);
        }

        private static long GetRecordCount(Table table, TransactionContext tx)
        {
            var count = table.Query(tx)
                .WithInMemoryOnly()
                .WithCommittedOnly()
                .Count();

            return count;
        }

        private List<TableRecordCount> GetTableRecordCounts(TransactionContext tx)
        {
            var tables = _tableProvider.GetTables(tx);
            var initialTables = tables
                .Select(t => new TableRecordCount(t, GetRecordCount(t, tx), false))
                //  We put >1 because persisting a unique record creates meta record in memory
                .Where(t => t.RecordCount > 1)
                .ToList();

            Sort(initialTables);

            return initialTables;
        }

        private bool MergeMetaRecords(TableRecordCount topCandidate, TransactionContext tx)
        {
            var blockMergingLogic = new BlockMergingLogic(Database);
            //var blockMergingLogic2 = new BlockMergingLogic2(Database);

            //blockMergingLogic2.CompactMerge(
            //    topCandidate.Table.Schema.TableName,
            //    null,
            //    tx);

            return blockMergingLogic.MergeBlocks(
                topCandidate.Table.Schema.TableName,
                null,
                Array.Empty<int>(),
                tx);
        }
    }
}
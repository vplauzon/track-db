using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TrackDb.Lib.DataLifeCycle.Persistance
{
    internal class TimePersistanceCandidateProvider : LogicBase, IPersistanceCandidateProvider
    {
        #region Inner types
        private record TableAge(Table Table, DateTime OldestCreationTime, bool IsMetaMerged);
        #endregion

        private readonly ITableProvider _tableProvider;
        private DateTime _lastPersistance = DateTime.Now;

        public TimePersistanceCandidateProvider(
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

            if (doPersistAll
                || _lastPersistance + Database.DatabasePolicy.InMemoryPolicy.MaxPersistancePeriod
                > DateTime.Now)
            {
                var tableRecordCounts = GetTableAges(tx);

                while (IsPersistanceRequired(tableRecordCounts, doPersistAll, tx))
                {
                    var topCandidate = tableRecordCounts.First();

                    tableRecordCounts.RemoveAt(0);
                    tx.LoadCommittedBlocksInTransaction(topCandidate.Table.Schema.TableName);

                    //  Validate a log merge didn't occur which change the counts
                    var newRecordCount = GetOldestCreationTime(topCandidate.Table, tx);

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
                            var newNewRecordCount = GetOldestCreationTime(topCandidate.Table, tx);

                            tableRecordCounts.Add(
                                new TableRecordCount(topCandidate.Table, newNewRecordCount, true));
                            Sort(tableRecordCounts);
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
        }

        private void Sort(List<TableAge> tableRecordCounts)
        {
            tableRecordCounts.Sort((trc1, trc2) => -trc1.RecordCount.CompareTo(trc2.RecordCount));
        }

        private bool IsPersistanceRequired(
            IEnumerable<TableAge> tableRecordCounts,
            bool doPersistAll,
            TransactionContext tx)
        {
            var totalRecords = tableRecordCounts
                .Sum(trc => trc.RecordCount);

            return totalRecords > _tableProvider.MaxInMemoryDataRecords
                || (totalRecords > 0 && doPersistAll);
        }

        private static DateTime? GetOldestCreationTime(Table table, TransactionContext tx)
        {
            var sortColumn = new SortColumn(table.Schema.CreationTimeColumnIndex, false);
            var oldestRecords = table.Query(tx)
                .WithInMemoryOnly()
                .WithCommittedOnly()
                .WithSortColumns([sortColumn])
                .WithProjection([table.Schema.CreationTimeColumnIndex])
                .Take(1);

            return oldestRecords;
        }

        private List<TableAge> GetTableAges(TransactionContext tx)
        {
            var tables = _tableProvider.GetTables(tx);
            var initialTables = tables
                .Select(t => new TableAge(t, GetOldestCreationTime(t, tx), false))
                .ToList();

            Sort(initialTables);

            return initialTables;
        }

        private bool MergeMetaRecords(TableAge topCandidate, TransactionContext tx)
        {
            var blockMergingLogic = new BlockMergingLogic(Database);

            return blockMergingLogic.MergeBlocks(
                topCandidate.Table.Schema.TableName,
                null,
                Array.Empty<int>(),
                tx);
        }
    }
}
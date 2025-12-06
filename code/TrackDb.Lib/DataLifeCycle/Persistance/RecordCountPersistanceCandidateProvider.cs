using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TrackDb.Lib.DataLifeCycle.Persistance
{
    internal class RecordCountPersistanceCandidateProvider : LogicBase, IPersistanceCandidateProvider
    {
        #region Inner types
        private record TableRecordCount(Table Table, long RecordCount);
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
            DataManagementActivity activity, TransactionContext tx)
        {
            var doPersistAll = _tableProvider.DoPersistAll(activity);
            var tableRecordCounts = GetTableRecordCounts(tx);

            while (IsPersistanceRequired(tableRecordCounts, doPersistAll, tx))
            {
                var topCandidate = tableRecordCounts.First();

                tableRecordCounts.RemoveAt(0);

                yield return new PersistanceCandidate(topCandidate.Table, doPersistAll);
            }
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

        private List<TableRecordCount> GetTableRecordCounts(TransactionContext tx)
        {
            var tables = _tableProvider.GetTables(tx);
            var initialTables = tables
                .Select(t => new TableRecordCount(t, t.Query(tx).WithInMemoryOnly().Count()))
                .OrderByDescending(trc => trc.RecordCount)
                .ToList();

            return initialTables;
        }
    }
}

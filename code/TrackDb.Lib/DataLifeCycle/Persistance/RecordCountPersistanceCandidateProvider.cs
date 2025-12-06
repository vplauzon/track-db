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
            var tableRecordCounts = GetTableRecordCounts(tx);

            while (IsPersistanceRequired(tableRecordCounts, activity, tx))
            {
            }
            throw new NotImplementedException();
        }

        private bool IsPersistanceRequired(
            IEnumerable<TableRecordCount> tableRecordCounts,
            DataManagementActivity activity,
            TransactionContext tx)
        {
            var totalRecords = tableRecordCounts
                .Sum(trc => trc.RecordCount);

            return totalRecords > _tableProvider.MaxInMemoryDataRecords
                || (totalRecords > 0 && _tableProvider.DoPersistAll(activity));
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

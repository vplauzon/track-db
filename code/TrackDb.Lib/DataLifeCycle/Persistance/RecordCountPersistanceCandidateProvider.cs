using System;
using System.Collections.Generic;
using System.Text;

namespace TrackDb.Lib.DataLifeCycle.Persistance
{
    internal class RecordCountPersistanceCandidateProvider : LogicBase, IPersistanceCandidateProvider
    {
        #region Inner types
        private record TableRecordCount(Table Table, int RecordCount);
        #endregion

        public RecordCountPersistanceCandidateProvider(Database database)
            : base(database)
        {
        }

        IEnumerable<PersistanceCandidate> IPersistanceCandidateProvider.FindCandidates(
            DataManagementActivity activity, TransactionContext tx)
        {
            throw new NotImplementedException();
        }
    }
}

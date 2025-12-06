using System;
using System.Collections.Generic;
using System.Text;

namespace TrackDb.Lib.DataLifeCycle.Persistance
{
    internal class TimePersistanceCandidateProvider : LogicBase, IPersistanceCandidateProvider
    {
        public TimePersistanceCandidateProvider(Database database)
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
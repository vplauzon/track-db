using System;
using System.Collections.Generic;
using System.Text;

namespace TrackDb.Lib.DataLifeCycle.Persistance
{
    internal class TimePersistanceCandidateProvider : IPersistanceCandidateProvider
    {
        IEnumerable<PersistanceCandidate> IPersistanceCandidateProvider.FindCandidates(
            DataManagementActivity activity, TransactionContext tx)
        {
            throw new NotImplementedException();
        }
    }
}

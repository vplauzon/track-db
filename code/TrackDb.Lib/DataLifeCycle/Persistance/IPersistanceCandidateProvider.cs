using System.Collections;
using System.Collections.Generic;

namespace TrackDb.Lib.DataLifeCycle.Persistance
{
    internal interface IPersistanceCandidateProvider
    {
        IEnumerable<PersistanceCandidate> FindCandidates(
            DataManagementActivity activity,
            TransactionContext tx);
    }
}
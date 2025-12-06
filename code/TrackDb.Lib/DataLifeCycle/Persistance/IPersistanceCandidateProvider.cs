using System.Collections;
using System.Collections.Generic;

namespace TrackDb.Lib.DataLifeCycle.Persistance
{
    internal interface IPersistanceCandidateProvider
    {
        bool IsPersistanceRequired(DataManagementActivity activity, TransactionContext tx);

        IEnumerable<Table> FindCandidates(DataManagementActivity activity, TransactionContext tx);
    }
}
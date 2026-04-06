using System;
using System.Collections.Generic;

namespace TrackDb.Lib.InMemory.Block
{
    internal record FilterOutput(
        IReadOnlyList<int> RowIndexes,
        IEnumerable<PredicateAuditTrail> PredicateAuditTrails);
}
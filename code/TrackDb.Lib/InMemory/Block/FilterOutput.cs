using System;
using System.Collections.Generic;

namespace TrackDb.Lib.InMemory.Block
{
    internal record FilterOutput(
        IEnumerable<int> RowIndexes,
        IEnumerable<PredicateAuditTrail> PredicateAuditTrails);
}
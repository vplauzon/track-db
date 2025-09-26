using System;
using TrackDb.Lib.Predicate;

namespace TrackDb.Lib.InMemory.Block
{
    internal record PredicateAuditTrail(DateTime Timestamp, QueryPredicate Predicate);
}
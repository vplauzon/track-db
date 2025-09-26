using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib.SystemData
{
    public record QueryExecutionRecord(
        DateTime Timestamp,
        string QueryId,
        string QueryTag,
        long BlockId,
        string Predicate);
}
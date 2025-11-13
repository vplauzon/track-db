using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib.Statistics
{
    public record InMemoryDataStatistics(int TableRecords, int TombstoneRecords);
}
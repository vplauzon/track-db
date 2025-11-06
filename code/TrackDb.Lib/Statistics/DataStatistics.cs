using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib.Statistics
{
    public record DataStatistics(
        int InMemoryTableRecords,
        int InMemoryTombstoneRecords,
        int OnDiskBlockCount,
        long OnDiskRecordCount,
        long OnDiskSize)
    {
        public long OnDiskSizePerBlock => OnDiskSize / OnDiskBlockCount;

        public long OnDiskSizePerRecord => OnDiskSize / OnDiskRecordCount;

        public long OnDiskRecordPerBlock => OnDiskRecordCount / OnDiskBlockCount;
    }
}
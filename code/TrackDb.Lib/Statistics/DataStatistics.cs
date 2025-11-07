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
        public long OnDiskSizePerBlock =>
            OnDiskBlockCount == 0 ? 0 : OnDiskSize / OnDiskBlockCount;

        public long OnDiskSizePerRecord =>
            OnDiskRecordCount == 0 ? 0 : OnDiskSize / OnDiskRecordCount;

        public long OnDiskRecordPerBlock =>
            OnDiskBlockCount == 0 ? 0 : OnDiskRecordCount / OnDiskBlockCount;
    }
}
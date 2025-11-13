using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib.Statistics
{
    public record PersistedDataStatistics(int BlockCount, long RecordCount, long Size)
    {
        public long BlockSize => BlockCount == 0 ? 0 : Size / BlockCount;

        public long RecordSize => RecordCount == 0 ? 0 : Size / RecordCount;

        public long RecordPerBlock => BlockCount == 0 ? 0 : RecordCount / BlockCount;
    }
}
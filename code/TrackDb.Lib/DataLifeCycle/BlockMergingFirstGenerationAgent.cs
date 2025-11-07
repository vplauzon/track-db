using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib.SystemData;

namespace TrackDb.Lib.DataLifeCycle
{
    internal class BlockMergingFirstGenerationAgent : BlockMergingAgentBase
    {
        public BlockMergingFirstGenerationAgent(
            Database database,
            TypedTable<TombstoneRecord> tombstoneTable,
            Lazy<DatabaseFileManager> storageManager)
            : base(database, tombstoneTable, storageManager)
        {
        }

        protected override bool RunMerge(bool doMergeAll)
        {
            throw new NotImplementedException();
        }
    }
}
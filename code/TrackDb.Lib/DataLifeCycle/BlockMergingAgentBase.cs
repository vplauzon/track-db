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
    internal abstract class BlockMergingAgentBase : DataLifeCycleAgentBase
    {
        #region Inner Types
        protected record MetadataRecord(
            int BlockId,
            int Size,
            long MinRecordId,
            ReadOnlyMemory<object?> Record);
        #endregion

        public BlockMergingAgentBase(
            Database database,
            TypedTable<TombstoneRecord> tombstoneTable,
            Lazy<DatabaseFileManager> storageManager)
            : base(database, tombstoneTable, storageManager)
        {
        }

        public override bool Run(DataManagementActivity forcedDataManagementActivity)
        {
            var doMergeAll =
                (forcedDataManagementActivity & DataManagementActivity.BlockMergeFirstGeneration) != 0;

            return RunMerge(doMergeAll);
        }

        protected abstract bool RunMerge(bool doMergeAll);
        
        protected bool MergeBlocks(
            IEnumerable<MetadataRecord> metadataRecords,
            int blockIdToMerge,
            bool doForceHardDelete)
        {
            throw new NotImplementedException();
        }
    }
}
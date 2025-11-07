using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib.InMemory.Block;
using TrackDb.Lib.SystemData;

namespace TrackDb.Lib.DataLifeCycle
{
    internal abstract class BlockMergingAgentBase : DataLifeCycleAgentBase
    {
        #region Inner Types
        protected record MetadataRecord(
            string metadataTableName,
            long RecordId,
            int BlockId,
            int Size,
            long MinRecordId,
            ReadOnlyMemory<object?> Record)
        {
            public static IEnumerable<MetadataRecord> LoadMetaRecords(IBlock block)
            {
                throw new NotImplementedException();
            }
        }
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
            var doPersistMetadata =
                (forcedDataManagementActivity & DataManagementActivity.PersistAllMetaDataFirstLevel) != 0;

            return RunMerge(doMergeAll, doPersistMetadata);
        }

        protected abstract bool RunMerge(bool doMergeAll, bool doPersistMetadata);

        protected bool MergeBlocks(
            IEnumerable<MetadataRecord> neighbours,
            MetadataRecord blockToMerge,
            bool doForceHardDelete)
        {
            throw new NotImplementedException();
        }
    }
}
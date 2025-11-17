using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib.SystemData;

namespace TrackDb.Lib.DataLifeCycle
{
    internal abstract class BlockMergingAgentBase : DataLifeCycleAgentBase
    {
        #region Inner types
        protected record Q(long? parentBlockId, long DeletedRecordId);
        #endregion

        public BlockMergingAgentBase(
            Database database,
            TypedTable<TombstoneRecord> tombstoneTable,
            Lazy<DatabaseFileManager> storageManager)
            : base(database, tombstoneTable, storageManager)
        {
        }

        protected void MergeBlocksUnder(string tableName, int metaBlockId, TransactionContext tx)
        {
            throw new NotImplementedException();
        }
    }
}
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib.InMemory.Block;
using TrackDb.Lib.Predicate;
using TrackDb.Lib.SystemData;

namespace TrackDb.Lib.DataLifeCycle
{
    internal abstract class DataLifeCycleAgentBase
    {
        private readonly Lazy<DatabaseFileManager> _storageManager;

        protected DataLifeCycleAgentBase(
            Database database,
            TypedTable<TombstoneRecord> tombstoneTable,
            Lazy<DatabaseFileManager> storageManager)
        {
            Database = database;
            TombstoneTable = tombstoneTable;
            _storageManager = storageManager;
        }

        /// <summary>Runs an agent logic.</summary>
        /// <param name="forcedDataManagementActivity"></param>
        /// <returns><c>true</c> iif the agent has run to completion and we can go to the next agent.</returns>
        public abstract bool Run(DataManagementActivity forcedDataManagementActivity);

        protected Database Database { get; }

        protected TypedTable<TombstoneRecord> TombstoneTable { get; }

        protected DatabaseFileManager StorageManager => _storageManager.Value;

        protected bool MergeTableTransactionLogs(string tableName)
        {
            using (var tx = Database.CreateTransaction())
            {
                var hasChanged = tx.LoadCommittedBlocksInTransaction(tableName);

                tx.Complete();

                return hasChanged;
            }
        }
    }
}
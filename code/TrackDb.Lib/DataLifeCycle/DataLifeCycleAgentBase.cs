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
        protected DataLifeCycleAgentBase(Database database)
        {
            Database = database;
        }

        /// <summary>Runs an agent logic.</summary>
        /// <param name="forcedDataManagementActivity"></param>
        /// <param name="tx"></param>
        public abstract void Run(
            DataManagementActivity forcedDataManagementActivity,
            TransactionContext tx);

        protected Database Database { get; }

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
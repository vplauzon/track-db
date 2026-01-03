using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib.Policies;
using TrackDb.Lib.Statistics;
using TrackDb.Lib.SystemData;

namespace TrackDb.Lib
{
    internal abstract class DatabaseContextBase
    {
        protected DatabaseContextBase(Database database)
        {
            Database = database;
        }

        protected Database Database { get; }

        public DatabasePolicy DatabasePolicy => Database.DatabasePolicy;

        public TransactionContext CreateTransaction()
        {
            return Database.CreateTransaction();
        }

        public async Task AwaitLifeCycleManagement(double tolerance)
        {
            await Database.AwaitLifeCycleManagement(tolerance);
        }

        public DatabaseStatistics GetDatabaseStatistics()
        {
            return Database.GetDatabaseStatistics();
        }

        public TypedTableQuery<QueryExecutionRecord> QueryQueryExecution(TransactionContext? tc = null)
        {
            return Database.QueryQueryExecution(tc);
        }
    }
}
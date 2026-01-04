using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib.Policies;
using TrackDb.Lib.Statistics;
using TrackDb.Lib.SystemData;

namespace TrackDb.Lib
{
    public abstract class DatabaseContextBase : IAsyncDisposable
    {
        protected DatabaseContextBase(Database database)
        {
            Database = database;
        }

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            await ((IAsyncDisposable)Database).DisposeAsync();
        }

        protected internal Database Database { get; }

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
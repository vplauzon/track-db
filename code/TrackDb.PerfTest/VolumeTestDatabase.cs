using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib;
using TrackDb.Lib.Policies;

namespace TrackDb.PerfTest
{
    internal class VolumeTestDatabase : IAsyncDisposable
    {
        #region Entity types
        public record Employee(string EmployeeId, string Name);

        public enum RequestStatus
        {
            Initiated,
            Treating,
            Completed
        }

        public record Request(
            string EmployeeId,
            string RequestCode,
            RequestStatus RequestStatus);

        public record Document(string RequestCode, string DocumentContent);
        #endregion

        private const string EMPLOYEE_TABLE = "Employee";
        private const string REQUEST_TABLE = "Request";
        private const string DOCUMENT_TABLE = "Document";

        #region Constructor
        public static async Task<VolumeTestDatabase> CreateAsync(
            Func<DatabasePolicy, DatabasePolicy>? dataPolicyChanger = null)
        {
            var dataPolicy = DatabasePolicy.Create();
            var modifiedDataPolicy = dataPolicyChanger != null
                ? dataPolicyChanger(dataPolicy)
                : dataPolicy;
            var db = await Database.CreateAsync<Database>(
                modifiedDataPolicy,
                TypedTableSchema<Employee>.FromConstructor(EMPLOYEE_TABLE)
                .AddPrimaryKeyProperty(p => p.EmployeeId),
                TypedTableSchema<Request>.FromConstructor(REQUEST_TABLE)
                .AddPrimaryKeyProperty(p => p.RequestCode),
                TypedTableSchema<Document>.FromConstructor(DOCUMENT_TABLE));

            return new VolumeTestDatabase(db);
        }

        private VolumeTestDatabase(Database database)
        {
            Database = database;
        }
        #endregion

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            await ((IAsyncDisposable)Database).DisposeAsync();
        }

        public Database Database { get; }

        public TypedTable<Employee> EmployeeTable
            => Database.GetTypedTable<Employee>(EMPLOYEE_TABLE);

        public TypedTable<Request> RequestTable
            => Database.GetTypedTable<Request>(REQUEST_TABLE);

        public TypedTable<Document> DocumentTable
            => Database.GetTypedTable<Document>(DOCUMENT_TABLE);
    }
}
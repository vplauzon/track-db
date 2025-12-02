using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib;
using TrackDb.Lib.Policies;

namespace TrackDb.UnitTest.VolumeTests
{
    internal class VolumeTestDatabase : IAsyncDisposable
    {
        #region Entity types
        public record Employee(int Id, string Name);
        #endregion

        private const string EMPLOYEE_TABLE = "Employee";

        #region Constructor
        public static async Task<VolumeTestDatabase> CreateAsync(
            Func<DatabasePolicy, DatabasePolicy>? dataPolicyChanger = null)
        {
            var dataPolicy = DatabasePolicy.Create(
                LifeCyclePolicy:LifeCyclePolicy.Create(MaxWaitPeriod:TimeSpan.FromSeconds(0)));
            var modifiedDataPolicy = dataPolicyChanger != null
                ? dataPolicyChanger(dataPolicy)
                : dataPolicy;
            var db = await Database.CreateAsync(
                modifiedDataPolicy,
                TypedTableSchema<Employee>.FromConstructor(EMPLOYEE_TABLE)
                .AddPrimaryKeyProperty(p => p.Id));

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
    }
}
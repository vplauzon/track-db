using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using TrackDb.Lib;
using TrackDb.Lib.Policies;
using Xunit;

namespace TrackDb.UnitTest.DbTests
{
    public class OptOutIndexingTest
    {
        #region Inner Types
        private record MyRecord(string Id, string Name, string Url);

        private class MyDatabase : DatabaseContextBase
        {
            public const string MY_RECORD_TABLE = "MyRecord";

            public static async Task<MyDatabase> CreateAsync(bool isOptingOut)
            {
                var tableSchema = TypedTableSchema<MyRecord>.FromConstructor(MY_RECORD_TABLE)
                    .AddPrimaryKeyProperty(p => p.Id);

                if (isOptingOut)
                {
                    tableSchema = tableSchema
                        .OptOutIndex(p => p.Name)
                        .OptOutIndex(p => p.Url);
                }

                var db = await Database.CreateAsync<MyDatabase>(
                    DatabasePolicy.Create(),
                    d => new(d),
                    CancellationToken.None,
                    tableSchema);

                return db;
            }

            private MyDatabase(Database database)
                : base(database)
            {
            }

            public TypedTable<MyRecord> RecordTable
                => Database.GetTypedTable<MyRecord>(MY_RECORD_TABLE);
        }
        #endregion

        [Fact]
        public async Task SimpleKey()
        {
            await using (var dbIndexed = await MyDatabase.CreateAsync(false))
            await using (var dbNotIndexed = await MyDatabase.CreateAsync(true))
            {
                var random = new Random();
                var records = Enumerable.Range(0, 100000)
                    .Select(i => new MyRecord(
                        $"id-{i}",
                        new string(Enumerable.Range(0, 10).Select(j => (char)(random.Next('Z' - 'A') + 'A')).ToArray()),
                        new string(Enumerable.Range(0, 100).Select(j => (char)(random.Next('Z' - 'A') + 'A')).ToArray())))
                    .ToImmutableArray();

                dbIndexed.RecordTable.AppendRecords(records);
                dbNotIndexed.RecordTable.AppendRecords(records);
                await dbIndexed.Database.AwaitLifeCycleManagementAsync(2, CancellationToken.None);
                await dbNotIndexed.Database.AwaitLifeCycleManagementAsync(2, CancellationToken.None);

                var indexedStats = dbIndexed.GetDatabaseStatistics();
                var notIndexedStats = dbNotIndexed.GetDatabaseStatistics();

                Assert.True(indexedStats.MaxTableGeneration >= notIndexedStats.MaxTableGeneration);

                var metaTableName = dbIndexed.Database.GetMetaDataTable(MyDatabase.MY_RECORD_TABLE)
                    .Schema
                    .TableName;
                var metaMetaTableName = dbIndexed.Database.GetMetaDataTable(metaTableName)
                    .Schema
                    .TableName;
                var indexedMetaRecordCount = dbIndexed.Database.GetAnyTable(metaMetaTableName)
                    .Query()
                    .Count();
                var notIndexedMetaRecordCount = dbNotIndexed.Database.GetAnyTable(metaMetaTableName)
                    .Query()
                    .Count();

                Assert.True(indexedMetaRecordCount >= notIndexedMetaRecordCount);
            }
        }
    }
}
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
    public class QueryInTest
    {
        #region Inner types
        private enum State
        {
            Open,
            Close,
            Undertermined
        }

        private class MyTestDatabase : DatabaseContextBase
        {
            private const string MY_RECORDS_TABLE = "MyRecords";

            public record MyRecord(
                int Integer,
                State State,
                DateTime Timestamp,
                bool IsReady,
                Uri BlobUri);

            #region Constructor
            public static async Task<MyTestDatabase> CreateAsync()
            {
                var db = await Database.CreateAsync<MyTestDatabase>(
                    DatabasePolicy.Create(),
                    db => new(db),
                    CancellationToken.None,
                    TypedTableSchema<MyRecord>.FromConstructor(MY_RECORDS_TABLE));

                return db;
            }

            private MyTestDatabase(Database database)
                : base(database)
            {
            }
            #endregion

            public TypedTable<MyRecord> MyRecordTable
                => Database.GetTypedTable<MyRecord>(MY_RECORDS_TABLE);
        }
        #endregion

        private static readonly DateTime _timestamp1 = DateTime.Now;
        private static readonly DateTime _timestamp2 = DateTime.Now.Subtract(TimeSpan.FromMinutes(5));
        private static readonly DateTime _timestamp3 = DateTime.Now.Subtract(TimeSpan.FromHours(5));

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task EnumIn(bool doPushPendingData)
        {
            await using (var db = await MyTestDatabase.CreateAsync())
            {
                InsertData(db);

                await db.Database.ForceDataManagementAsync(doPushPendingData
                    ? DataManagementActivity.PersistAllNonMetaData
                    : DataManagementActivity.None);

                var results = db.MyRecordTable.Query()
                    .Where(pf => pf.In(r => r.State, [State.Open, State.Undertermined]))
                    .Select(r => r.Integer)
                    .ToImmutableList();

                Assert.Equal(2, results.Count);
                Assert.Contains(1, results);
                Assert.Contains(3, results);
            }
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task DateTimeIn(bool doPushPendingData)
        {
            await using (var db = await MyTestDatabase.CreateAsync())
            {
                InsertData(db);

                await db.Database.ForceDataManagementAsync(doPushPendingData
                    ? DataManagementActivity.PersistAllNonMetaData
                    : DataManagementActivity.None);

                var results = db.MyRecordTable.Query()
                    .Where(pf => pf.In(r => r.Timestamp, [_timestamp1, _timestamp2]))
                    .Select(r => r.Integer)
                    .ToImmutableList();

                Assert.Equal(2, results.Count);
                Assert.Contains(1, results);
                Assert.Contains(2, results);
            }
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task BoolIn(bool doPushPendingData)
        {
            await using (var db = await MyTestDatabase.CreateAsync())
            {
                InsertData(db);

                await db.Database.ForceDataManagementAsync(doPushPendingData
                    ? DataManagementActivity.PersistAllNonMetaData
                    : DataManagementActivity.None);

                var results = db.MyRecordTable.Query()
                    .Where(pf => pf.In(r => r.IsReady, [true]))
                    .Select(r => r.Integer)
                    .ToImmutableList();

                Assert.Equal(2, results.Count);
                Assert.Contains(1, results);
                Assert.Contains(3, results);
            }
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task UriIn(bool doPushPendingData)
        {
            await using (var db = await MyTestDatabase.CreateAsync())
            {
                InsertData(db);

                await db.Database.ForceDataManagementAsync(doPushPendingData
                    ? DataManagementActivity.PersistAllNonMetaData
                    : DataManagementActivity.None);

                var results = db.MyRecordTable.Query()
                    .Where(pf => pf.In(r => r.BlobUri, [new Uri("http://Bob")]))
                    .Select(r => r.Integer)
                    .ToImmutableList();

                Assert.Single(results);
                Assert.Contains(2, results);
            }
        }

        private static void InsertData(MyTestDatabase db)
        {
            using (var tx = db.CreateTransaction())
            {
                db.MyRecordTable.AppendRecord(
                    new MyTestDatabase.MyRecord(
                        1,
                        State.Open,
                        _timestamp1,
                        true,
                        new Uri("http://Alice")),
                    tx);
                db.MyRecordTable.AppendRecord(
                    new MyTestDatabase.MyRecord(
                        2,
                        State.Close,
                        _timestamp2,
                        false,
                        new Uri("http://Bob")),
                    tx);
                db.MyRecordTable.AppendRecord(
                    new MyTestDatabase.MyRecord(
                        3,
                        State.Undertermined,
                        _timestamp3,
                        true,
                        new Uri("http://Carl")),
                    tx);

                tx.Complete();
            }
        }
    }
}
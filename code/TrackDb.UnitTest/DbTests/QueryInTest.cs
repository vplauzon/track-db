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

            public record MyRecord(int Integer, State State);

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

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task EnumIn(bool doPushPendingData)
        {
            await using (var db = await MyTestDatabase.CreateAsync())
            {
                db.MyRecordTable.AppendRecord(new MyTestDatabase.MyRecord(1, State.Open));
                db.MyRecordTable.AppendRecord(new MyTestDatabase.MyRecord(2, State.Close));
                db.MyRecordTable.AppendRecord(new MyTestDatabase.MyRecord(3, State.Undertermined));

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
    }
}
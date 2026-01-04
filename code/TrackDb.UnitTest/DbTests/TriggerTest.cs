using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using TrackDb.Lib;
using TrackDb.Lib.Policies;
using Xunit;

namespace TrackDb.UnitTest.DbTests
{
    public class TriggerTest
    {
        #region Inner types
        public record MainEntity(string Name, string Category, int Value);

        /// <summary>
        /// This is there just so we can make changes without main-entity triggers being invoked.
        /// </summary>
        /// <param name="Name"></param>
        public record SubEntity(string Name);

        public record TriggerCount(int Count);

        private class TestTriggerDatabaseContext : DatabaseContextBase
        {
            private const string MAIN_ENTITY_TABLE = "MainEntity";
            private const string SUB_ENTITY_TABLE = "SubEntity";
            private const string TRIGGER_COUNT_TABLE = "TriggerCount";

            private TestTriggerDatabaseContext(Database db)
                : base(db)
            {
            }

            public static async Task<TestTriggerDatabaseContext> CreateAsync()
            {
                var db = await Database.CreateAsync(
                    DatabasePolicy.Create(),
                    db => new TestTriggerDatabaseContext(db),
                    TypedTableSchema<MainEntity>.FromConstructor(MAIN_ENTITY_TABLE)
                    .AddTrigger((genDb, tx) =>
                    {
                        var db = (TestTriggerDatabaseContext)genDb;

                        db.TriggerCount.AppendRecord(new TriggerCount(1));
                    }),
                    TypedTableSchema<SubEntity>.FromConstructor(SUB_ENTITY_TABLE),
                    TypedTableSchema<TriggerCount>.FromConstructor(TRIGGER_COUNT_TABLE));

                return db;
            }

            public TypedTable<MainEntity> MainEntity =>
                Database.GetTypedTable<MainEntity>(MAIN_ENTITY_TABLE);

            public TypedTable<SubEntity> SubEntity =>
                Database.GetTypedTable<SubEntity>(SUB_ENTITY_TABLE);

            public TypedTable<TriggerCount> TriggerCount =>
                Database.GetTypedTable<TriggerCount>(TRIGGER_COUNT_TABLE);
        }
        #endregion

        [Fact]
        public async Task NoChange()
        {
            await using (var db = await TestTriggerDatabaseContext.CreateAsync())
            {
                var record = new SubEntity("Bob");

                db.SubEntity.AppendRecord(record);

                Assert.Equal(0, db.TriggerCount.Query().Count());
            }
        }

        [Fact]
        public async Task OneChange()
        {
            await using (var db = await TestTriggerDatabaseContext.CreateAsync())
            {
                var record = new MainEntity("Bob", "Employee", 42);

                db.MainEntity.AppendRecord(record);

                Assert.Equal(1, db.TriggerCount.Query().Count());
            }
        }

        [Fact]
        public async Task TwoChanges()
        {
            await using (var db = await TestTriggerDatabaseContext.CreateAsync())
            {
                var record1 = new MainEntity("Alice", "Employee", 74);
                var record2 = new MainEntity("Bob", "Employee", 42);

                db.MainEntity.AppendRecord(record1);
                db.MainEntity.AppendRecord(record2);

                Assert.Equal(1, db.TriggerCount.Query().Count());
            }
        }
    }
}
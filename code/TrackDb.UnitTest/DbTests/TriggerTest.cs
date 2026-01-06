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
    public class TriggerTest
    {
        #region Inner types
        public record MainEntity(string Name, string Category, int Value);

        public record MainEntityAccumulation(string Category, int SumValue);

        /// <summary>
        /// This is there just so we can make changes without main-entity triggers being invoked.
        /// </summary>
        /// <param name="Name"></param>
        public record SubEntity(string Name);

        public record TriggerCount(int Count);

        private class TestTriggerDatabaseContext : DatabaseContextBase
        {
            private const string MAIN_ENTITY_TABLE = "MainEntity";
            private const string MAIN_ENTITY_ACCUMULATION_TABLE = "MainEntityAccumulation";
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
                    CancellationToken.None,
                    TypedTableSchema<MainEntity>.FromConstructor(MAIN_ENTITY_TABLE)
                    //  Just count
                    .AddTrigger((genDb, tx) =>
                    {
                        var db = (TestTriggerDatabaseContext)genDb;

                        db.TriggerCount.AppendRecord(new TriggerCount(1));
                    })
                    //  Accumulation
                    .AddTrigger((genDb, tx) =>
                    {
                        var db = (TestTriggerDatabaseContext)genDb;
                        var accumulations = db.MainEntity.Query(tx)
                        .WithinTransactionOnly()
                        .GroupBy(m => m.Category)
                        .Select(g => new MainEntityAccumulation(g.Key, g.Sum(m => m.Value)));
                        var decumulations = db.MainEntity.TombstonedWithinTransaction(tx)
                        .GroupBy(m => m.Category)
                        .Select(g => new MainEntityAccumulation(g.Key, -g.Sum(m => m.Value)));
                        var integratedAccumulations = accumulations
                        .Concat(decumulations)
                        .GroupBy(m => m.Category)
                        .Select(g => new MainEntityAccumulation(g.Key, g.Sum(m => m.SumValue)));

                        db.MainEntityAccumulation.AppendRecords(integratedAccumulations, tx);
                    }),
                    TypedTableSchema<MainEntityAccumulation>.FromConstructor(MAIN_ENTITY_ACCUMULATION_TABLE),
                    TypedTableSchema<SubEntity>.FromConstructor(SUB_ENTITY_TABLE),
                    TypedTableSchema<TriggerCount>.FromConstructor(TRIGGER_COUNT_TABLE));

                return db;
            }

            public TypedTable<MainEntity> MainEntity =>
                Database.GetTypedTable<MainEntity>(MAIN_ENTITY_TABLE);

            public TypedTable<SubEntity> SubEntity =>
                Database.GetTypedTable<SubEntity>(SUB_ENTITY_TABLE);

            public TypedTable<MainEntityAccumulation> MainEntityAccumulation =>
                Database.GetTypedTable<MainEntityAccumulation>(MAIN_ENTITY_ACCUMULATION_TABLE);

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

                Assert.Equal(2, db.TriggerCount.Query().Count());
            }
        }

        [Fact]
        public async Task OneChangeOneDelete()
        {
            await using (var db = await TestTriggerDatabaseContext.CreateAsync())
            {
                var record = new MainEntity("Alice", "Employee", 74);

                db.MainEntity.AppendRecord(record);
                db.MainEntity.Query().Delete();

                Assert.Equal(2, db.TriggerCount.Query().Count());
            }
        }

        [Fact]
        public async Task PureAccumulation()
        {
            await using (var db = await TestTriggerDatabaseContext.CreateAsync())
            {
                var record1 = new MainEntity("Alice", "Employee", 74);
                var record2 = new MainEntity("Bob", "Employee", 42);
                var record3 = new MainEntity("Carl", "Employee", 10);
                var record4 = new MainEntity("Dominic", "Employee", 16);

                db.MainEntity.AppendRecord(record1);
                using (var tx = db.CreateTransaction())
                {
                    db.MainEntity.AppendRecord(record2, tx);
                    db.MainEntity.AppendRecord(record3, tx);
                    //  Done outside the transaction, in parallel
                    db.MainEntity.AppendRecord(record4);

                    tx.Complete();
                }

                Assert.Equal(3, db.MainEntityAccumulation.Query().Count());
                Assert.Equal(
                    record1.Value + record2.Value + record3.Value + record4.Value,
                    db.MainEntityAccumulation.Query().Sum(a => a.SumValue));
            }
        }

        [Fact]
        public async Task AccumulationWithDeletion()
        {
            await using (var db = await TestTriggerDatabaseContext.CreateAsync())
            {
                var record1 = new MainEntity("Alice", "Employee", 74);
                var record2 = new MainEntity("Bob", "Employee", 42);
                var record3 = new MainEntity("Carl", "Employee", 10);
                var record4 = new MainEntity("Dominic", "Employee", 16);

                db.MainEntity.AppendRecord(record1);
                using (var tx = db.CreateTransaction())
                {
                    db.MainEntity.AppendRecord(record2, tx);
                    db.MainEntity.AppendRecord(record3, tx);
                    //  Done outside the transaction, in parallel
                    db.MainEntity.AppendRecord(record4);

                    tx.Complete();
                }
                //  Delete record3
                db.MainEntity.Query()
                    .Where(pf => pf.Equal(r => r.Value, record3.Value))
                    .Delete();

                Assert.Equal(4, db.MainEntityAccumulation.Query().Count());
                Assert.Equal(
                    record1.Value + record2.Value + record4.Value,
                    db.MainEntityAccumulation.Query().Sum(a => a.SumValue));
            }
        }
    }
}
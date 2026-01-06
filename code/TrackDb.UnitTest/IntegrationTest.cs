using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using TrackDb.Lib;
using TrackDb.Lib.Policies;
using Xunit;

namespace TrackDb.UnitTest
{
    public class IntegrationTest
    {
        #region Inner types
        private enum Status
        {
            Starting,
            InProgress,
            Completed
        }

        private record Entity(
            string Name,
            Status? Status,
            int Step,
            long Ticks,
            short LegacyId,
            DateTime? Timestamp);

        private class TestDatabaseContext : DatabaseContextBase
        {
            private const string ENTITY_TABLE = "Entity";

            private TestDatabaseContext(Database db)
                : base(db)
            {
            }

            public static async Task<TestDatabaseContext> CreateAsync()
            {
                var db = await Database.CreateAsync<TestDatabaseContext>(
                    DatabasePolicy.Create(),
                    db => new TestDatabaseContext(db),
                    CancellationToken.None,
                    TypedTableSchema<Entity>.FromConstructor(ENTITY_TABLE));

                return db;
            }

            public TypedTable<Entity> Entity => Database.GetTypedTable<Entity>(ENTITY_TABLE);
        }
        #endregion

        [Theory]
        [InlineData(false, false)]
        [InlineData(true, false)]
        [InlineData(false, true)]
        [InlineData(true, true)]
        public async Task ComprehensiveAsync(bool doPersistAllUserData, bool doHardDeleteAll)
        {
            var dataManagementActivity =
                (doPersistAllUserData ? DataManagementActivity.PersistAllNonMetaData : DataManagementActivity.None)
                |
                (doHardDeleteAll ? DataManagementActivity.HardDeleteAll : DataManagementActivity.None);
            var db = await TestDatabaseContext.CreateAsync();
            var table = db.Entity;
            var entity1 = new Entity("Alice", Status.InProgress, 25, 450000000, 657, DateTime.Now);
            var entity2 = new Entity("Bob", Status.Starting, 13, 320000000, 892, DateTime.Now);
            //  Added but deleted
            var entity3 = new Entity("Carl", Status.Completed, 89, 720000000, 123, DateTime.Now);
            var entity4 = new Entity("Diana", null, 65, 540000000, 785, DateTime.Now);

            table.AppendRecords([entity1, entity3]);
            table.AppendRecord(entity2);

            await db.Database.ForceDataManagementAsync(dataManagementActivity);

            table.Query()
                .WherePredicate(pf => pf.Equal(e => e.Name, entity3.Name))
                .Delete();
            table.AppendRecord(entity4);

            await db.Database.ForceDataManagementAsync(dataManagementActivity);

            var result1 = table.Query()
                .WherePredicate(pf => pf.GreaterThan(e => e.Step, 20))
                .ToImmutableArray()
                .OrderBy(e => e.Name)
                .ToImmutableArray();

            Assert.Equal(2, result1.Count());
            Assert.Equal(entity1.Name, result1[0].Name);
            Assert.Equal(entity1.Status, result1[0].Status);
            Assert.Equal(entity1.Step, result1[0].Step);
            Assert.Equal(entity1.Ticks, result1[0].Ticks);
            Assert.Equal(entity1.LegacyId, result1[0].LegacyId);
            Assert.Equal(entity1.Timestamp, result1[0].Timestamp);
            Assert.Equal(entity4.Name, result1[1].Name);
            Assert.Equal(entity4.Status, result1[1].Status);
            Assert.Equal(entity4.Step, result1[1].Step);
            Assert.Equal(entity4.Ticks, result1[1].Ticks);
            Assert.Equal(entity4.LegacyId, result1[1].LegacyId);
            Assert.Equal(entity4.Timestamp, result1[1].Timestamp);

            var result2 = table.Query()
                .WherePredicate(pf => pf.LessThanOrEqual(e => e.Ticks, entity1.Ticks))
                .OrderBy(e => e.Name)
                .ToImmutableArray();

            Assert.Equal(2, result2.Count());
            Assert.Equal(entity1.Name, result2[0].Name);
            Assert.Equal(entity1.Status, result2[0].Status);
            Assert.Equal(entity1.Step, result2[0].Step);
            Assert.Equal(entity1.Ticks, result2[0].Ticks);
            Assert.Equal(entity1.LegacyId, result2[0].LegacyId);
            Assert.Equal(entity1.Timestamp, result2[0].Timestamp);
            Assert.Equal(entity2.Name, result2[1].Name);
            Assert.Equal(entity2.Status, result2[1].Status);
            Assert.Equal(entity2.Step, result2[1].Step);
            Assert.Equal(entity2.Ticks, result2[1].Ticks);
            Assert.Equal(entity2.LegacyId, result2[1].LegacyId);
            Assert.Equal(entity2.Timestamp, result2[1].Timestamp);

            var result3 = table.Query()
                .WherePredicate(pf => pf.LessThan(e => e.LegacyId, entity2.LegacyId))
                .OrderByDesc(e => e.Step)
                .Take(1)
                .ToImmutableArray();

            Assert.Single(result3);
            Assert.Equal(entity4.Name, result3[0].Name);
            Assert.Equal(entity4.Status, result3[0].Status);
            Assert.Equal(entity4.Step, result3[0].Step);
            Assert.Equal(entity4.Ticks, result3[0].Ticks);
            Assert.Equal(entity4.LegacyId, result3[0].LegacyId);
            Assert.Equal(entity4.Timestamp, result3[0].Timestamp);

            var result4 = table.Query()
                .WherePredicate(pf => pf.LessThan(e => e.Timestamp, entity3.Timestamp))
                .OrderByDesc(e => e.Timestamp)
                .Take(1)
                .ToImmutableArray();

            Assert.Single(result4);
            Assert.Equal(entity2.Name, result4[0].Name);
            Assert.Equal(entity2.Status, result4[0].Status);
            Assert.Equal(entity2.Step, result4[0].Step);
            Assert.Equal(entity2.Ticks, result4[0].Ticks);
            Assert.Equal(entity2.LegacyId, result4[0].LegacyId);
            Assert.Equal(entity2.Timestamp, result4[0].Timestamp);

            var result5 = table.Query()
                .WherePredicate(pf => pf.NotEqual(e => e.Status, Status.Completed))
                .OrderByDesc(e => e.Status)
                .ToImmutableArray();

            Assert.Equal(3, result5.Length);
            Assert.Equal(entity1.Name, result5[0].Name);
            Assert.Equal(entity1.Status, result5[0].Status);
            Assert.Equal(entity1.Step, result5[0].Step);
            Assert.Equal(entity1.Ticks, result5[0].Ticks);
            Assert.Equal(entity1.LegacyId, result5[0].LegacyId);
            Assert.Equal(entity1.Timestamp, result5[0].Timestamp);
            Assert.Equal(entity2.Name, result5[1].Name);
            Assert.Equal(entity2.Status, result5[1].Status);
            Assert.Equal(entity2.Step, result5[1].Step);
            Assert.Equal(entity2.Ticks, result5[1].Ticks);
            Assert.Equal(entity2.LegacyId, result5[1].LegacyId);
            Assert.Equal(entity2.Timestamp, result5[1].Timestamp);
            Assert.Equal(entity4.Name, result5[2].Name);
            Assert.Equal(entity4.Status, result5[2].Status);
            Assert.Equal(entity4.Step, result5[2].Step);
            Assert.Equal(entity4.Ticks, result5[2].Ticks);
            Assert.Equal(entity4.LegacyId, result5[2].LegacyId);
            Assert.Equal(entity4.Timestamp, result5[2].Timestamp);
        }
    }
}
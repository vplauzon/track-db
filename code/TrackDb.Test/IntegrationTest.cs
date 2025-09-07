using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib;
using Xunit;

namespace TrackDb.Test
{
    public class IntegrationTest
    {
        #region Inner types
        private record Entity(
            string Name,
            int Step);
        #endregion

        [Theory]
        [InlineData(false, false)]
        [InlineData(true, false)]
        [InlineData(false, true)]
        [InlineData(true, true)]
        public async Task ComprehensiveAsync(bool doPersistAllUserData, bool doHardDeleteAll)
        {
            var dataManagementActivity =
                (doPersistAllUserData ? DataManagementActivity.PersistAllUserData : DataManagementActivity.None)
                &
                (doHardDeleteAll ? DataManagementActivity.HardDeleteAll : DataManagementActivity.None); 
            var tableName = "MyTable";
            var db = new Database(TypedTableSchema<Entity>.FromConstructor(tableName));
            var table = db.GetTypedTable<Entity>(tableName);
            var entity1 = new Entity("Alice", 25);
            var entity2 = new Entity("Bob", 13);
            //  Added but deleted
            var entity3 = new Entity("Carl", 89);
            var entity4 = new Entity("Diana", 65);

            table.AppendRecords([entity1, entity3]);
            table.AppendRecord(entity2);

            await db.ForceDataManagementAsync(dataManagementActivity);

            table.Query()
                .Where(table.PredicateFactory.Equal(e => e.Name, entity3.Name))
                .Delete();
            table.AppendRecord(entity4);

            await db.ForceDataManagementAsync(dataManagementActivity);

            var result1 = table.Query()
                .Where(table.PredicateFactory.GreaterThan(e => e.Step, 20))
                .ToImmutableArray()
                .OrderBy(e => e.Name)
                .ToImmutableArray();

            Assert.Equal(2, result1.Count());
            Assert.Equal(entity1.Name, result1[0].Name);
            Assert.Equal(entity1.Step, result1[0].Step);
            Assert.Equal(entity4.Name, result1[1].Name);
            Assert.Equal(entity4.Step, result1[1].Step);

            var result2 = table.Query()
                .Where(table.PredicateFactory.LessThanOrEqual(e => e.Step, entity1.Step))
                .OrderBy(e => e.Name)
                .ToImmutableArray();

            Assert.Equal(2, result2.Count());
            Assert.Equal(entity1.Name, result2[0].Name);
            Assert.Equal(entity1.Step, result2[0].Step);
            Assert.Equal(entity2.Name, result2[1].Name);
            Assert.Equal(entity2.Step, result2[1].Step);

            var result3 = table.Query()
                .Where(table.PredicateFactory.LessThan(e => e.Step, entity4.Step))
                .OrderByDesc(e => e.Step)
                .Take(1)
                .ToImmutableArray();

            Assert.Single(result3);
            Assert.Equal(entity1.Name, result3[0].Name);
            Assert.Equal(entity1.Step, result3[0].Step);
        }
    }
}
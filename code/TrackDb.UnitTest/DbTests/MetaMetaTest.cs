using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using TrackDb.Lib;
using Xunit;

namespace TrackDb.UnitTest.DbTests
{
    public class MetaMetaTest
    {
        [Fact]
        public async Task AppendOnly()
        {
            await using (var db = await TestDatabase.CreateAsync())
            {
                var record1 = new TestDatabase.Primitives(1);
                var record2 = new TestDatabase.Primitives(2);

                db.PrimitiveTable.AppendRecord(record1);
                db.PrimitiveTable.AppendRecord(record2);
                await db.Database.ForceDataManagementAsync(DataManagementActivity.PersistAllNonMetaData);
                await db.Database.ForceDataManagementAsync(DataManagementActivity.PersistAllMetaDataFirstLevel);

                var state = db.Database.GetDatabaseStateSnapshot();
                var map = state.TableMap;
                var metadataTableName = map[db.PrimitiveTable.Schema.TableName].MetaDataTableName;

                Assert.NotNull(metadataTableName);

                var metaMetadataTableName = map[metadataTableName].MetaDataTableName;

                Assert.Null(metaMetadataTableName);

                var record3 = new TestDatabase.Primitives(3);
                var record4 = new TestDatabase.Primitives(4);

                db.PrimitiveTable.AppendRecord(record3);
                db.PrimitiveTable.AppendRecord(record4);
                await db.Database.ForceDataManagementAsync(DataManagementActivity.PersistAllNonMetaData);
                await db.Database.ForceDataManagementAsync(DataManagementActivity.PersistAllMetaDataFirstLevel);

                state = db.Database.GetDatabaseStateSnapshot();
                map = state.TableMap;

                metaMetadataTableName = map[metadataTableName].MetaDataTableName;

                //  Meta records got merged
                Assert.Null(metaMetadataTableName);
            }
        }

        [Fact]
        public async Task WithQuery()
        {
            await using (var db = await TestDatabase.CreateAsync())
            {
                var forcedActivity = DataManagementActivity.PersistAllNonMetaData
                    | DataManagementActivity.PersistAllMetaDataFirstLevel;
                var record1 = new TestDatabase.Primitives(1);
                var record2 = new TestDatabase.Primitives(2);
                var record3 = new TestDatabase.Primitives(3);

                db.PrimitiveTable.AppendRecord(record1);
                await db.Database.ForceDataManagementAsync(forcedActivity);
                db.PrimitiveTable.AppendRecord(record2);
                await db.Database.ForceDataManagementAsync(forcedActivity);
                db.PrimitiveTable.AppendRecord(record3);
                await db.Database.ForceDataManagementAsync(forcedActivity);

                var allRecords = db.PrimitiveTable.Query()
                    .ToImmutableArray();

                Assert.Equal(3, allRecords.Length);
                Assert.True(
                    Enumerable.SequenceEqual(
                        [1, 2, 3],
                        allRecords
                        .Select(p => p.Integer)
                        .Order()));

                foreach (var record in allRecords)
                {
                    var queryRecords = db.PrimitiveTable.Query()
                        .Where(pf => pf.Equal(r => r.Integer, record.Integer))
                        .ToImmutableArray();

                    Assert.Single(queryRecords);
                    Assert.Equal(record.Integer, queryRecords.First().Integer);
                }
            }
        }

        [Fact]
        public async Task WithManyUpdates()
        {
            await using (var db = await TestDatabase.CreateAsync())
            {
                var forcedActivity = DataManagementActivity.PersistAllNonMetaData
                    | DataManagementActivity.PersistAllMetaDataFirstLevel;
                var record1 = new TestDatabase.Primitives(1);
                var record2 = new TestDatabase.Primitives(2);
                var record3 = new TestDatabase.Primitives(3);
                var record4 = new TestDatabase.Primitives(4);
                var record5 = new TestDatabase.Primitives(5);
                var record6 = new TestDatabase.Primitives(6);

                db.PrimitiveTable.AppendRecord(record1);
                await db.Database.ForceDataManagementAsync(forcedActivity);
                db.PrimitiveTable.AppendRecord(record2);
                await db.Database.ForceDataManagementAsync(forcedActivity);
                db.PrimitiveTable.AppendRecord(record3);
                await db.Database.ForceDataManagementAsync(forcedActivity);
                db.PrimitiveTable.UpdateRecord(record1, record4);
                await db.Database.ForceDataManagementAsync(forcedActivity);
                db.PrimitiveTable.UpdateRecord(record2, record5);
                await db.Database.ForceDataManagementAsync(forcedActivity);
                db.PrimitiveTable.UpdateRecord(record3, record6);
                await db.Database.ForceDataManagementAsync(forcedActivity);

                var allRecords = db.PrimitiveTable.Query()
                    .ToImmutableArray();

                Assert.Equal(3, allRecords.Length);
                Assert.True(
                    Enumerable.SequenceEqual(
                        [4, 5, 6],
                        allRecords
                        .Select(p => p.Integer)
                        .Order()));


                foreach (var record in allRecords)
                {
                    var queryRecords = db.PrimitiveTable.Query()
                        .Where(pf => pf.Equal(r => r.Integer, record.Integer))
                        .ToImmutableArray();

                    Assert.Single(queryRecords);
                    Assert.Equal(record.Integer, queryRecords.First().Integer);
                }
            }
        }

        [Fact]
        public async Task WithHardDelete()
        {
            await using (var db = await TestDatabase.CreateAsync())
            {
                var record1 = new TestDatabase.Primitives(1);
                var record2 = new TestDatabase.Primitives(2);
                var record3 = new TestDatabase.Primitives(3);

                db.PrimitiveTable.AppendRecord(record1);
                await db.Database.ForceDataManagementAsync(
                    DataManagementActivity.PersistAllNonMetaData);
                db.PrimitiveTable.AppendRecord(record2);
                await db.Database.ForceDataManagementAsync(
                    DataManagementActivity.PersistAllNonMetaData);
                db.PrimitiveTable.AppendRecord(record3);
                await db.Database.ForceDataManagementAsync(
                    DataManagementActivity.PersistAllNonMetaData);
                await db.Database.ForceDataManagementAsync(
                    DataManagementActivity.PersistAllMetaDataFirstLevel);

                db.PrimitiveTable.Query()
                    .Where(pf=>pf.Equal(p => p.Integer, record2.Integer))
                    .Delete();
                //  This will trigger a rebuild of metablocks
                await db.Database.ForceDataManagementAsync(
                    DataManagementActivity.HardDeleteAll);

                var allRecords = db.PrimitiveTable.Query()
                    .ToImmutableArray();

                Assert.Equal(2, allRecords.Length);
                Assert.True(
                    Enumerable.SequenceEqual(
                        [1, 3],
                        allRecords
                        .Select(p => p.Integer)
                        .Order()));
            }
        }
    }
}
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
        public async Task MetaMetaTable()
        {
            await using (var db = await TestDatabase.CreateAsync())
            {
                var forcedActivity = DataManagementActivity.PersistAllNonMetaData
                    | DataManagementActivity.PersistAllMetaDataFirstLevel;
                var record = new TestDatabase.Primitives(1);

                db.PrimitiveTable.AppendRecord(record);
                await db.Database.ForceDataManagementAsync(forcedActivity);

                var state = db.Database.GetDatabaseStateSnapshot();
                var map = state.TableMap;
                var metadataTableName = map[db.PrimitiveTable.Schema.TableName].MetaDataTableName;

                Assert.NotNull(metadataTableName);

                var metaMetadataTableName = map[metadataTableName].MetaDataTableName;
            
                Assert.NotNull(metaMetadataTableName);

                var metaMetaMetadataTableName = map[metaMetadataTableName].MetaDataTableName;

                Assert.Null(metaMetaMetadataTableName);
            }
        }
    }
}
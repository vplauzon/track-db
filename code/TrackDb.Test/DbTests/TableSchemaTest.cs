using TrackDb.Lib;
using System;
using Xunit;

namespace TrackDb.Tests.DbTests
{
    public class TableSchemaTest
    {
        #region Inner types
        private record IntOnly(int Integer);
        #endregion

        [Fact]
        public void TestOnlyInt()
        {
            const string TABLE_NAME = "MyTable";
            var schema = TypedTableSchema<IntOnly>.FromConstructor(TABLE_NAME);

            Assert.Equal(TABLE_NAME, schema.TableName);
            Assert.Empty(schema.PartitionKeyColumnIndexes);

            schema = schema.AddPartitionKeyProperty(o => o.Integer);

            Assert.Single(schema.PartitionKeyColumnIndexes);
            Assert.Equal(0, schema.PartitionKeyColumnIndexes[0]);

            Assert.Single(schema.Columns);
            Assert.Equal("Integer", schema.Columns[0].ColumnName);
            Assert.Equal(typeof(int), schema.Columns[0].ColumnType);
        }
    }
}
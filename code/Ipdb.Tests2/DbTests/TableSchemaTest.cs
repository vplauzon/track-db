using Ipdb.Lib2;
using System;
using Xunit;

namespace Ipdb.Tests2.DbTests
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
            var schema = new TableSchema<IntOnly>(TABLE_NAME);

            Assert.Equal(TABLE_NAME, schema.TableName);
            Assert.Equal(typeof(IntOnly), schema.RepresentationType);
            Assert.Empty(schema.PartitionKeyPropertyPaths);

            schema = schema.AddPartitionKeyProperty(o => o.Integer);

            Assert.Single(schema.PartitionKeyPropertyPaths);
            Assert.Equal(nameof(IntOnly.Integer), schema.PartitionKeyPropertyPaths[0]);

            Assert.Single(schema.Columns);
            Assert.Equal("Integer", schema.Columns[0].PropertyPath);
            Assert.Equal(typeof(int), schema.Columns[0].ColumnType);
        }
    }
}
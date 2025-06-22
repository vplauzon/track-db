using Ipdb.Lib2;
using System;
using Xunit;

namespace Ipdb.Tests2
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
            Assert.Empty(schema.PrimaryKeys);

            schema = schema.AddPrimaryKey(o => o.Integer);

            Assert.Single(schema.PrimaryKeys);
            Assert.Equal(nameof(IntOnly.Integer), schema.PrimaryKeys[0]);

            Assert.Single(schema.Columns);
            Assert.Equal("Integer", schema.Columns[0].PropertyName);
            Assert.Equal(typeof(int), schema.Columns[0].ColumnType);
        }
    }
}
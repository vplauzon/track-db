using Ipdb.Lib2;
using Xunit;

namespace Ipdb.Tests2
{
    public class TableSchemaTest
    {
        #region Inner types
        private readonly record struct OnlyInt(int Integer);
        #endregion

        [Fact]
        public void TestOnlyInt()
        {
            const string TABLE_NAME = "MyTable";
            var schema = new TableSchema<OnlyInt>(TABLE_NAME);

            Assert.Equal(TABLE_NAME, schema.TableName);
            Assert.Equal(typeof(OnlyInt), schema.RepresentationType);
            Assert.Empty(schema.PrimaryKeys);

            schema = schema.AddPrimaryKey(o => o.Integer);

            Assert.Single(schema.PrimaryKeys);
            Assert.Equal(nameof(OnlyInt.Integer), schema.PrimaryKeys[0]);
        }
    }
}
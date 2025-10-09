using System;
using System.Linq.Expressions;
using TrackDb.Lib;
using Xunit;

namespace TrackDb.UnitTest
{
    public class TableSchemaTest
    {
        #region Inner types
        private record IntOnly(int Integer);

        private record NestedProperties(InnerClass Inner);

        private record InnerClass(string Name, MoreInnerClass More);

        private record MoreInnerClass(int Value);
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

        [Fact]
        public void TestNestedPropertyAccess()
        {
            const string TABLE_NAME = "NestedTable";

            var schema = TypedTableSchema<NestedProperties>.FromConstructor(TABLE_NAME);

            // Verify schema creation with nested properties
            Assert.Equal(TABLE_NAME, schema.TableName);
            Assert.Contains(schema.Columns, c => c.ColumnName == "Inner.Name");
            Assert.Contains(schema.Columns, c => c.ColumnName == "Inner.More.Value");

            // Test single-level property access
            Expression<Func<NestedProperties, string>> expression1 = p => p.Inner.Name;

            Assert.Single(schema.GetColumnIndexSubset(expression1));
            Assert.Equal(0, schema.GetColumnIndexSubset(expression1)[0]);

            // Test multi-level property access
            Expression<Func<NestedProperties, int>> expression2 = p => p.Inner.More.Value;

            Assert.Single(schema.GetColumnIndexSubset(expression2));
            Assert.Equal(1, schema.GetColumnIndexSubset(expression2)[0]);

            Expression<Func<NestedProperties, InnerClass>> expression3 = p => p.Inner;
            
            Assert.Equal(2, schema.GetColumnIndexSubset(expression3).Count);
            Assert.Equal(0, schema.GetColumnIndexSubset(expression3)[0]);
            Assert.Equal(1, schema.GetColumnIndexSubset(expression3)[1]);
        }
    }
}
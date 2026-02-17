using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using TrackDb.Lib;
using TrackDb.Lib.InMemory.Block;
using Xunit;

namespace TrackDb.UnitTest.DbTests
{
    public class BlockSerializationTest
    {
        [Fact]
        public async Task ReadWrite()
        {
            var schema = new TableSchema(
                "Thingy",
                [new ColumnSchema("Name", typeof(string)), new ColumnSchema("Id", typeof(int))],
                Array.Empty<int>(),
                Array.Empty<int>(),
                Array.Empty<TableTriggerAction>());
            var blockBuilder = new BlockBuilder(schema);
            var buffer = new byte[5000];

            blockBuilder.AppendRecord(1, ["Alice", 12]);
            blockBuilder.AppendRecord(2, ["Bob", 42]);
            blockBuilder.AppendRecord(3, ["Carl", 5]);

            var stats = blockBuilder.Serialize(buffer);
            var block = ReadOnlyBlock.Load(buffer, schema);

            Assert.Equal(stats.ItemCount, block.RecordCount);

            var records = block.Project(
                new object?[3],
                [0, 1, schema.RecordIdColumnIndex],
                Enumerable.Range(0, stats.ItemCount),
                1042)
                .Select(r => r.ToArray())
                .ToImmutableArray();

            Assert.Equal("Alice", records[0][0]);
            Assert.Equal(12, records[0][1]);

            Assert.Equal("Bob", records[1][0]);
            Assert.Equal(42, records[1][1]);

            Assert.Equal("Carl", records[2][0]);
            Assert.Equal(5, records[2][1]);
        }
    }
}
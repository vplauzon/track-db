using System;
using System.Linq;
using TrackDb.Lib;
using TrackDb.Lib.InMemory.Block;
using TrackDb.Lib.Predicate;
using Xunit;

namespace TrackDb.UnitTest
{
    public class BlockTest
    {
        [Fact]
        public void Append()
        {
            var schema = new TableSchema(
                "Person",
                [
                    new ColumnSchema("Id", typeof(int)),
                    new ColumnSchema("Name", typeof(string)),
                    new ColumnSchema("Timestamp", typeof(DateTime))],
                [],
                [],
                []);
            var block1 = new BlockBuilder(schema);
            var block2 = new BlockBuilder(schema);
            var block3 = new BlockBuilder(schema);
            var record1 = new object?[] { 42, "Alice", DateTime.Now };
            var record2 = new object?[] { 43, "Bob", DateTime.Now };
            var record3 = new object?[] { 44, "Carl", DateTime.Now };
            var record4 = new object?[] { 45, "Dan", DateTime.Now };

            block1.AppendRecord(1, record1);
            block1.AppendRecord(2, record2);

            block2.AppendRecord(3, record3);

            block3.AppendRecord(4, record4);

            var superBlock = BlockBuilder.MergeBlocks(block1, block2, block3);

            Assert.Equal(4, ((IBlock)superBlock).RecordCount);

            ReadOnlySpan<object?> RecordRetriever(int id)
            {
                IBlock block = superBlock;
                var predicate = new BinaryOperatorPredicate(
                    0,
                    id,
                    BinaryOperator.Equal);
                var filterOutput = block.Filter(predicate, false);

                Assert.Single(filterOutput.RowIndexes);

                var records = block.Project(new object?[3], [0, 1, 2], filterOutput.RowIndexes, 0);

                return records.First().Span;
            }

            var retrieved1 = RecordRetriever((int)record1[0]!);
            var retrieved2 = RecordRetriever((int)record2[0]!);
            var retrieved3 = RecordRetriever((int)record3[0]!);
            var retrieved4 = RecordRetriever((int)record4[0]!);

            Assert.True(record1.SequenceEqual(retrieved1));
            Assert.True(record2.SequenceEqual(retrieved2));
            Assert.True(record3.SequenceEqual(retrieved3));
            Assert.True(record4.SequenceEqual(retrieved4));
        }

        [Fact]
        public void AppendBigString()
        {
            var schema = new TableSchema(
                "Person",
                [
                    new ColumnSchema("Id", typeof(int)),
                    new ColumnSchema("Name", typeof(string))],
                [],
                [],
                []);
            var block = new BlockBuilder(schema);
            var record = new object?[2];
            var random = new Random();

            for (var i = 0; i != 100; ++i)
            {
                record[0] = i;
                record[1] = new string(Enumerable.Range(0, 300)
                    .Select(i => (char)random.Next('a', 'z'))
                    .ToArray());
                block.AppendRecord(i, record);
            }

            var segments = block.SegmentRecords(4096);

            Assert.True(segments.Count > 1);
        }
    }
}
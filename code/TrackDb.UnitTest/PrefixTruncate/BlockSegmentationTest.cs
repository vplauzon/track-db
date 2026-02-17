using TrackDb.Lib;
using TrackDb.Lib.InMemory.Block;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace TrackDb.UnitTest.PrefixTruncate
{
    public class BlockSegmentationTest
    {
        const int MAX_SIZE = 4000;

        private readonly TableSchema _schema = new(
            "MyTable",
            [new ColumnSchema("MyColumn", typeof(int))],
            Array.Empty<int>(),
            Array.Empty<int>(),
            Array.Empty<TableTriggerAction>());

        [Fact]
        public void OneRowData()
        {
            var block = new BlockBuilder(_schema);

            block.AppendRecord(1, new[] { (object)1 });

            var segments = block.SegmentRecords(MAX_SIZE);

            Assert.Single(segments);
            Assert.Equal(1, segments[0].ItemCount);
            Assert.True(segments[0].Size <= MAX_SIZE);
        }

        [Fact]
        public void ManyRowsData()
        {
            const int ROW_COUNT = 100000;

            var block = new BlockBuilder(_schema);

            for (var i = 1; i != ROW_COUNT; ++i)
            {
                block.AppendRecord(i, [(object)i]);
            }

            var segments = block.SegmentRecords(MAX_SIZE);

            Assert.True(segments.Count > 0);
            Assert.True(segments.All(s => s.ItemCount < ROW_COUNT));
            Assert.True(segments.All(s => s.Size <= MAX_SIZE));
        }
    }
}

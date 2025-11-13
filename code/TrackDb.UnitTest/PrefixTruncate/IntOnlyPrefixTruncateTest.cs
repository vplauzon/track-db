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
    public class IntOnlyPrefixTruncateTest
    {
        const int MAX_SIZE = 4000;

        private readonly TableSchema _schema = new(
            "MyTable",
            new[] { new ColumnSchema("MyColumn", typeof(int)) },
            Array.Empty<int>(),
            Array.Empty<int>());

        [Fact]
        public void OneRowData()
        {
            var block = new BlockBuilder(_schema);

            block.AppendRecord(DateTime.Now, 1, new[] { (object)1 });

            var blockStats = block.TruncateSerialize(new byte[MAX_SIZE], 0);

            Assert.Equal(1, blockStats.ItemCount);
            Assert.True(blockStats.Size <= MAX_SIZE);
        }

        [Fact]
        public void ManyRowsData()
        {
            const int ROW_COUNT = 100000;

            var block = new BlockBuilder(_schema);

            for (var i = 1; i != ROW_COUNT; ++i)
            {
                block.AppendRecord(DateTime.Now, i, [(object)i]);
            }

            var blockStats = block.TruncateSerialize(new byte[MAX_SIZE], 0);

            Assert.True(blockStats.ItemCount > 0);
            Assert.True(blockStats.ItemCount < ROW_COUNT);
            Assert.True(blockStats.Size <= MAX_SIZE);
        }
    }
}

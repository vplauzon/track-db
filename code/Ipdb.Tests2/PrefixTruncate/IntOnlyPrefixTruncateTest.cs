using Ipdb.Lib2;
using Ipdb.Lib2.Cache.CachedBlock;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Ipdb.Tests2.PrefixTruncate
{
    public class IntOnlyPrefixTruncateTest
    {
        const int MAX_SIZE = 4000;

        private readonly TableSchema _schema = new(
            "MyTable",
            new[] { new ColumnSchema("MyColumn", typeof(int)) },
            Array.Empty<int>());

        [Fact]
        public void NoData()
        {
            var block = new BlockBuilder(_schema);
            var prefix = block.TruncateBlock(MAX_SIZE);

            Assert.Equal(0, ((IBlock)prefix).RecordCount);
        }

        [Fact]
        public void OneRowData()
        {
            var block = new BlockBuilder(_schema);

            block.AppendRecord(1, new[] { (object)1 });

            var prefix = block.TruncateBlock(MAX_SIZE);

            Assert.Equal(1, ((IBlock)prefix).RecordCount);
            Assert.True(prefix.Serialize().Payload.Length <= MAX_SIZE);
        }

        [Fact]
        public void ManyRowsData()
        {
            const int ROW_COUNT = 100000;
            
            var block = new BlockBuilder(_schema);

            for (var i = 1; i != ROW_COUNT; ++i)
            {
                block.AppendRecord(i, new[] { (object)i });
            }

            var prefix = block.TruncateBlock(MAX_SIZE);

            Assert.True(((IBlock)prefix).RecordCount > 0);
            Assert.True(((IBlock)prefix).RecordCount < ROW_COUNT);
            Assert.True(prefix.Serialize().Payload.Length <= MAX_SIZE);
        }
    }
}

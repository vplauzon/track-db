using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace TrackDb.PerfTest
{
    public class InsertTest : InsertUpdateTestBase
    {
        public InsertTest()
            : base(false)
        {
        }

        [Fact]
        public async Task Test000010()
        {
            await RunPerformanceTestAsync(10);
        }

        [Fact]
        public async Task Test000100()
        {
            await RunPerformanceTestAsync(100);
        }

        [Fact]
        public async Task Test001000()
        {
            await RunPerformanceTestAsync(1000);
        }

        [Fact]
        public async Task Test010000()
        {
            await RunPerformanceTestAsync(10000);
        }
    }
}
using Ipdb.Lib2.Cache.CachedBlock.SpecializedColumn;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Ipdb.Tests2.Codecs
{
    public class Int64CodecTest
    {
        [Fact]
        public void IdenticalNonNull()
        {
            TestScenario(new long?[] { 42, 42, 42 }, false);
        }

        [Fact]
        public void IdenticalWithNull()
        {
            TestScenario(new long?[] { 42, null, 42 }, true);
        }

        [Fact]
        public void OnlyNulls()
        {
            TestScenario(new long?[] { null, null, null, null }, false);
        }

        [Fact]
        public void VariedNonNull()
        {
            TestScenario(new long?[] { 1, 2, 3, 4, 5, 6 }, true);
        }

        [Fact]
        public void VariedWithNull()
        {
            TestScenario(new long?[] { 1, null, 3, 4, null }, true);
        }

        [Fact]
        public void LongOne()
        {
            //  Fixed seed for reproductability
            var random = new Random(42);

            TestScenario(
                Enumerable.Range(0, 25000)
                .Select(i => (long?)random.Next(0, 25000))
                .ToImmutableArray(),
                true);
        }

        private static void TestScenario(IEnumerable<long?> data, bool doExpectPayload)
        {
            var bundle = Int64Codec.Compress(data);
            var decodedArray = Int64Codec.Decompress(bundle)
                .ToImmutableArray();

            Assert.Equal(doExpectPayload, bundle.Payload.Length != 0);
            Assert.True(Enumerable.SequenceEqual(decodedArray, data));
            Assert.Equal(data.Min(), decodedArray.Min());
            Assert.Equal(data.Max(), decodedArray.Max());
        }
    }
}
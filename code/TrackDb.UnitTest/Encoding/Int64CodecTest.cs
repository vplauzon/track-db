using TrackDb.Lib.InMemory.Block.SpecializedColumn;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using TrackDb.Lib.Encoding;

namespace TrackDb.UnitTest.Encoding
{
    public class Int64CodecTest
    {
        [Fact]
        public void IdenticalNonNull()
        {
            TestScenario(new long?[] { 42, 42, 42 });
        }

        [Fact]
        public void IdenticalWithNull()
        {
            TestScenario(new long?[] { 42, null, 42 });
        }

        [Fact]
        public void IdenticalWithOneNonNull()
        {
            TestScenario(new long?[] { null, null, 43 });
        }

        [Fact]
        public void OnlyNulls()
        {
            TestScenario(new long?[] { null, null, null, null });
        }

        [Fact]
        public void VariedNonNull()
        {
            TestScenario(new long?[] { 1, 2, 3, 4, 5, 6 });
        }

        [Fact]
        public void VariedWithNull()
        {
            TestScenario(new long?[] { 1, null, 3, 4, null });
        }

        [Fact]
        public void LongOne()
        {
            //  Fixed seed for reproductability
            var random = new Random(42);

            TestScenario(
                Enumerable.Range(0, 25000)
                .Select(i => (long?)random.Next(0, 25000))
                .ToImmutableArray());
        }

        [Fact]
        public void SequenceTooLarge()
        {
            var data = Enumerable.Range(0, UInt16.MaxValue + 1)
                .Select(i => (long)i)
                .ToArray();

            Assert.Throws<ArgumentOutOfRangeException>(() =>
            {
                var writer = new ByteWriter(new byte[4 * UInt16.MaxValue]);

                Int64Codec.Compress(data, long.MinValue, ref writer);
            });
        }

        [Fact]
        public void SingleValueSequences()
        {
            TestScenario(new long?[] { 42 });
            TestScenario(new long?[] { null });
        }

        [Fact]
        public void TwoValueSequences()
        {
            TestScenario(new long?[] { 1, 2 });
            TestScenario(new long?[] { null, 1 });
            TestScenario(new long?[] { 1, null });
            TestScenario(new long?[] { 1, 1 });
        }

        [Fact]
        public void BoundaryValues()
        {
            TestScenario(new long?[] { long.MinValue, 0, long.MaxValue });
            TestScenario(new long?[] { long.MinValue, null, long.MaxValue });

            // Large deltas
            TestScenario(new long?[] { 0, 1000000000, 2000000000, 3000000000 });

            // Values requiring maximum bit width
            TestScenario(new long?[] { long.MinValue, long.MinValue / 2, 0,
                long.MaxValue / 2, long.MaxValue });
        }

        [Fact]
        public void BitmapEdgeCases()
        {
            // Exactly 8 items (one byte bitmap)
            TestScenario(new long?[] { 1, null, 2, null, 3, null, 4, null });

            // 9 items (spans two bytes)
            TestScenario(new long?[] { 1, null, 2, null, 3, null, 4, null, 5 });

            // Null at byte boundary
            TestScenario(new long?[] { 1, 2, 3, 4, 5, 6, 7, null, 9, 10 });
        }

        [Fact]
        public void CompressionEfficiency()
        {
            // Small deltas
            TestScenario(new long?[] { 1000, 1001, 1002, 1003, 1004 });

            // Large deltas
            TestScenario(new long?[] { 1000, 2000, 3000, 4000, 5000 });

            // Alternating pattern
            TestScenario(new long?[] { 1, null, 2, null, 3, null, 4, null });

            // Mostly null
            TestScenario(new long?[] { null, null, null, 42, null, null });

            // Mostly values
            TestScenario(new long?[] { 1, 2, 3, null, 4, 5, 6 });
        }

        private static void TestScenario(IEnumerable<long?> data)
        {
            var nullValue = long.MinValue;
            var dataArray = data
                .Select(d => d == null ? nullValue : d.Value)
                .ToArray();
            var buffer = new byte[50000];
            var writer = new ByteWriter(buffer);
            var reader = new ByteReader(buffer);
            var package = Int64Codec.Compress(dataArray, nullValue, ref writer);
            var decodedArray = new long[dataArray.Length];

            Int64Codec.Decompress(ref reader, decodedArray, nullValue);

            Assert.True(Enumerable.SequenceEqual(decodedArray, dataArray));
        }
    }
}

using TrackDb.Lib.InMemory.Block.SpecializedColumn;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace TrackDb.UnitTest.Codecs
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
        public void IdenticalWithOneNonNull()
        {
            TestScenario(new long?[] { null, null, 43 }, true);
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

        [Fact]
        public void EmptySequence()
        {
            Assert.Throws<ArgumentNullException>(() => Int64Codec.Compress(null!));
            Assert.Throws<ArgumentNullException>(() => Int64Codec.Compress(Array.Empty<long?>()));
        }

        [Fact]
        public void SequenceTooLarge()
        {
            var data = Enumerable.Range(0, UInt16.MaxValue + 1)
                .Select(i => (long?)i)
                .ToArray();
            Assert.Throws<ArgumentOutOfRangeException>(() => Int64Codec.Compress(data));
        }

        [Fact]
        public void SingleValueSequences()
        {
            TestScenario(new long?[] { 42 }, false);
            TestScenario(new long?[] { null }, false);
        }

        [Fact]
        public void TwoValueSequences()
        {
            TestScenario(new long?[] { 1, 2 }, true);
            TestScenario(new long?[] { 1, null }, true);
            TestScenario(new long?[] { null, 1 }, true);
            TestScenario(new long?[] { 1, 1 }, false);
        }

        [Fact]
        public void BoundaryValues()
        {
            TestScenario(new long?[] { long.MinValue, 0, long.MaxValue }, true);
            TestScenario(new long?[] { long.MinValue, null, long.MaxValue }, true);
            
            // Large deltas
            TestScenario(new long?[] { 0, 1000000000, 2000000000, 3000000000 }, true);
            
            // Values requiring maximum bit width
            TestScenario(new long?[] { long.MinValue, long.MinValue / 2, 0, 
                long.MaxValue / 2, long.MaxValue }, true);
        }

        [Fact]
        public void BitmapEdgeCases()
        {
            // Exactly 8 items (one byte bitmap)
            TestScenario(new long?[] { 1, null, 2, null, 3, null, 4, null }, true);
            
            // 9 items (spans two bytes)
            TestScenario(new long?[] { 1, null, 2, null, 3, null, 4, null, 5 }, true);
            
            // Null at byte boundary
            TestScenario(new long?[] { 1, 2, 3, 4, 5, 6, 7, null, 9, 10 }, true);
        }

        [Fact]
        public void CompressionEfficiency()
        {
            // Small deltas
            TestScenario(new long?[] { 1000, 1001, 1002, 1003, 1004 }, true);
            
            // Large deltas
            TestScenario(new long?[] { 1000, 2000, 3000, 4000, 5000 }, true);
            
            // Alternating pattern
            TestScenario(new long?[] { 1, null, 2, null, 3, null, 4, null }, true);
            
            // Mostly null
            TestScenario(new long?[] { null, null, null, 42, null, null }, true);
            
            // Mostly values
            TestScenario(new long?[] { 1, 2, 3, null, 4, 5, 6 }, true);
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

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib.Encoding;
using Xunit;

namespace TrackDb.UnitTest.Encoding
{
    public class BitPackerTest
    {
        [Fact]
        public void BasicScenarios()
        {
            var random = new Random();
            var scenarios = new IEnumerable<ulong>[]
            {
                new ulong[] { 0, 1 },
                new ulong[] { 0, 0, 0, 1 },
                new ulong[] { 0, 1, 2, 3, 4, 5, 6 },
                new ulong[] { 0, 42 },
                Enumerable.Range(0, 25000)
                .Select(i => (ulong)random.Next(0, 25000))
                .ToImmutableArray(),
            };

            TestSequences(scenarios);
        }

        [Fact]
        public void EdgeCases()
        {
            var scenarios = new IEnumerable<ulong>[]
            {
                // Empty sequence
                Array.Empty<ulong>(),
                
                // Single value sequences
                new ulong[] { 0 },
                new ulong[] { 42 },
                new ulong[] { 255 },
                
                // All same values
                Enumerable.Repeat(7UL, 100).ToArray(),
                
                // Maximum values requiring specific bits
                new ulong[] { 1, 0, 1, 1 },              // 1 bit
                new ulong[] { 3, 0, 2, 1 },              // 2 bits
                new ulong[] { 15, 8, 7, 4 },             // 4 bits
                new ulong[] { 255, 128, 64, 32 },        // 8 bits
                new ulong[] { 65535, 32768, 16384 },     // 16 bits
                
                // Maximum possible ulong value test
                new ulong[] { ulong.MaxValue, 0, ulong.MaxValue/2, ulong.MaxValue }
            };

            TestSequences(scenarios);
        }

        [Fact]
        public void ByteBoundaryTests()
        {
            // Create sequences that span byte boundaries
            var scenarios = new IEnumerable<ulong>[]
            {
                // 3 bits per value = spans bytes
                new ulong[] { 7, 7, 7, 7, 7, 7, 7, 7 },
                
                // 6 bits per value = aligns with bytes sometimes
                new ulong[] { 63, 63, 63, 63, 63, 63, 63, 63 },
                
                // 12 bits per value = always spans bytes
                new ulong[] { 4095, 4095, 4095, 4095 },
                
                // Mix of values needing different bits
                new ulong[] { 4095, 255, 15, 3 }
            };

            TestSequences(scenarios);
        }

        [Fact]
        public void StressTest()
        {
            var random = new Random(42); // Fixed seed for reproducibility
            var scenarios = new IEnumerable<ulong>[]
            {
                // Large sequence with varied values
                Enumerable.Range(0, 100000)
                    .Select(i => (ulong)random.Next(0, 1000000))
                    .ToArray(),
                
                // Large sequence with small values (dense packing)
                Enumerable.Range(0, 100000)
                    .Select(i => (ulong)random.Next(0, 8))
                    .ToArray(),
                
                // Large sequence with large values (sparse packing)
                Enumerable.Range(0, 10000)
                    .Select(i => (ulong)random.Next(1000000, 2000000))
                    .ToArray()
            };

            TestSequences(scenarios);
        }

        private void TestSequences(IEnumerable<IEnumerable<ulong>> scenarios)
        {
            foreach (var originalSequence in scenarios)
            {
                var max = originalSequence.Any() ? originalSequence.Max() : 0UL;
                var packedArray = new byte[BitPacker.PackSize(originalSequence.Count(), max)];
                var writer = new ByteWriter(packedArray);

                BitPacker.Pack(
                    originalSequence,
                    originalSequence.Count(),
                    max,
                    ref writer);

                var unpackedArray = BitPacker.Unpack(
                    packedArray,
                    originalSequence.Count(),
                    max)
                    .ToImmutableArray(v => v);

                Assert.True(Enumerable.SequenceEqual(unpackedArray, originalSequence));
            }
        }
    }
}

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
    public class BitPackerTest
    {
        [Fact]
        public void BasicScenarios()
        {
            var random = new Random();
            var scenarios = new IEnumerable<long>[]
            {
                new long[] { 0, 0, 0, 1 },
                new long[] { 1,3,4 },
                new long[] { 0, 1, 2, 3, 4, 5, 6 },
                new long[] { 0, 42 },
                Enumerable.Range(0, 25000)
                .Select(i=>(long)random.Next(0, 25000))
                .ToImmutableArray(),
            };

            TestSequences(scenarios);
        }

        [Fact]
        public void EdgeCases()
        {
            var scenarios = new IEnumerable<long>[]
            {
                // Empty sequence
                Array.Empty<long>(),
                
                // Single value sequences
                new long[] { 0 },
                new long[] { 42 },
                new long[] { 255 },
                
                // All same values
                Enumerable.Repeat(7L, 100).ToArray(),
                
                // Maximum values requiring specific bits
                new long[] { 1, 0, 1, 1 },              // 1 bit
                new long[] { 3, 0, 2, 1 },              // 2 bits
                new long[] { 15, 8, 7, 4 },             // 4 bits
                new long[] { 255, 128, 64, 32 },        // 8 bits
                new long[] { 65535, 32768, 16384 },     // 16 bits
            };

            TestSequences(scenarios);
        }

        [Fact]
        public void ByteBoundaryTests()
        {
            // Create sequences that span byte boundaries
            var scenarios = new IEnumerable<long>[]
            {
                // 3 bits per value = spans bytes
                new long[] { 7, 7, 7, 7, 7, 7, 7, 7 },
                
                // 6 bits per value = aligns with bytes sometimes
                new long[] { 63, 63, 63, 63, 63, 63, 63, 63 },
                
                // 12 bits per value = always spans bytes
                new long[] { 4095, 4095, 4095, 4095 },
                
                // Mix of values needing different bits
                new long[] { 4095, 255, 15, 3 }
            };

            TestSequences(scenarios);
        }

        [Fact]
        public void StressTest()
        {
            var random = new Random(42); // Fixed seed for reproducibility
            var scenarios = new IEnumerable<long>[]
            {
                // Large sequence with varied values
                Enumerable.Range(0, 100000)
                    .Select(i => (long)random.Next(0, 1000000))
                    .ToArray(),
                
                // Large sequence with small values (dense packing)
                Enumerable.Range(0, 100000)
                    .Select(i => (long)random.Next(0, 8))
                    .ToArray(),
                
                // Large sequence with large values (sparse packing)
                Enumerable.Range(0, 10000)
                    .Select(i => (long)random.Next(1000000, 2000000))
                    .ToArray()
            };

            TestSequences(scenarios);
        }

        private void TestSequences(IEnumerable<IEnumerable<long>> scenarios)
        {
            foreach (var originalSequence in scenarios)
            {
                var min = originalSequence.Any() ? originalSequence.Min() : 0;
                var max = originalSequence.Any() ? originalSequence.Max() : 0;

                var packedArray = BitPacker.Pack(
                    originalSequence.Select(i => i - min),
                    originalSequence.Count(),
                    max - min);
                var unpackedArray = BitPacker.Unpack(
                    packedArray,
                    originalSequence.Count(),
                    max - min)
                    .Select(i => i + min)
                    .ToImmutableArray();

                Assert.True(Enumerable.SequenceEqual(unpackedArray, originalSequence));
            }
        }
    }
}

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
        public void Scenarios()
        {
            var random = new Random();
            var scenarios = new IEnumerable<long>[]
            {
                new long[] { 0, 0, 0, 1 },
                new long[] { 0, 1, 2, 3, 4, 5, 6 },
                new long[] { 0, 42 },
                Enumerable.Range(0, 25000)
                .Select(i=>(long)random.Next(0, 25000))
                .ToImmutableArray(),
            };

            foreach (var originalSequence in scenarios)
            {
                var packedArray = BitPacker.Pack(
                    originalSequence,
                    originalSequence.Count(),
                    originalSequence.Max());
                var unpackedArray = BitPacker.Unpack(
                    packedArray,
                    originalSequence.Count(),
                    originalSequence.Max())
                    .ToImmutableArray();

                Assert.True(Enumerable.SequenceEqual(unpackedArray, originalSequence));
            }
        }
    }
}
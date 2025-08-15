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
        public void Scenarios()
        {
            var random = new Random();
            var scenarios = new IEnumerable<long?>[]
            {
                new long?[] { 42, 42, 42 },
                new long?[] { 42, null, 42 },
                new long?[] { 1, 2, 3, 4, 5, 6 },
                new long?[] { 1, null, 3, 4, null },
                new long?[] { null, null, null, null },
                Enumerable.Range(0, 25000)
                .Select(i=>(long?)random.Next(0, 25000))
                .ToImmutableArray(),
            };

            foreach (var originalSequence in scenarios)
            {
                var bundle = Int64Codec.Compress(originalSequence);
                var decodedArray = Int64Codec.Decompress(bundle)
                    .ToImmutableArray();

                Assert.True(Enumerable.SequenceEqual(decodedArray, originalSequence));
                Assert.Equal(originalSequence.Min(), decodedArray.Min());
                Assert.Equal(originalSequence.Max(), decodedArray.Max());
            }
        }
    }
}
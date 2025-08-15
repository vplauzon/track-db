using Ipdb.Lib2.Cache.CachedBlock.SpecializedColumn;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Ipdb.Tests2
{
    public class Int64CodecTest
    {
        [Fact]
        public void Scenarios()
        {
            var scenarios = new[]
            {
                new long?[] { 1, 2, 3, 4, 5, 6 },
                new long?[] { 1, null, 3, 4, null },
                new long?[] { null, null, null, null }
            };

            foreach(var array in scenarios)
            {
                var bundle = Int64Codec.Compress(array);
                var decodedArray = Int64Codec.Decompress(bundle)
                    .ToImmutableArray();

                Assert.True(Enumerable.SequenceEqual(decodedArray, array));
                Assert.Equal(array.Min(), decodedArray.Min());
                Assert.Equal(array.Max(), decodedArray.Max());
            }
        }
    }
}
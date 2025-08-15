using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ipdb.Lib2.Cache.CachedBlock.SpecializedColumn
{
    internal static class BitPacker
    {
        /// <summary>
        /// Packs a sequence of <see cref="long"/>s.  Each item is encoded into a sequence of bits.
        /// Number of bits per item is dictated by the range of value, which is 0 to
        /// <paramref name="maximumValue"/>.
        /// </summary>
        /// <param name="data">
        /// Sequence of <see cref="long"/> items with minimum value of 0 and maximum value of
        /// <paramref name="maximumValue"/>.
        /// </param>
        /// <param name="dataCount">The number of items in <paramref name="data"/>.</param>
        /// <param name="maximumValue">Maximum value in <paramref name="data"/>.</param>
        /// <returns>Byte array of bit-packed representation of <paramref name="data"/>.</returns>
        public static byte[] Pack(IEnumerable<long> data, int dataCount, long maximumValue)
        {
            throw new NotImplementedException();
        }
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib.Encoding
{
    /// <summary>
    /// Packs / unpacks <see cref="ulong"/> array in bytes by leveraging the real number of bytes
    /// needed given the range (min/max) of integers in the array.
    /// </summary>
    internal static class BitPacker
    {
        /// <summary>
        /// Calculates the size (in bytes) to pack <paramref name="itemCount"/> <see cref="ulong"/>
        /// given they are all between 0 and <paramref name="maximumValue"/>.
        /// </summary>
        /// <param name="itemCount"></param>
        /// <param name="maximumValue"></param>
        /// <returns></returns>
        public static int PackSize(int itemCount, ulong maximumValue)
        {
            return PackSizeAdvanced(itemCount, maximumValue).TotalBytes;
        }

        /// <summary>
        /// Returns more size information than <see cref="PackSize"/>.  Mostly for internal purposes.
        /// </summary>
        /// <param name="itemCount"></param>
        /// <param name="maximumValue"></param>
        /// <returns></returns>
        public static (int TotalBytes, int BitsPerValue) PackSizeAdvanced(
            int itemCount,
            ulong maximumValue)
        {
            // Calculate number of bits needed per value
            var bitsPerValue = maximumValue == ulong.MaxValue
                ? 64
                : (int)Math.Ceiling(Math.Log2(maximumValue + 1));
            // Calculate total bits and bytes needed
            var totalBits = itemCount * bitsPerValue;
            var totalBytes = (totalBits + 7) / 8; // Round up to nearest byte

            return (totalBytes, bitsPerValue);
        }

        /// <summary>
        /// Packs a sequence of <see cref="ulong"/>s.  Each item is encoded into a sequence of bits.
        /// Number of bits per item is dictated by the range of value, which is 0 to
        /// <paramref name="maximumValue"/>.
        /// </summary>
        /// <param name="data">
        /// Sequence of <see cref="ulong"/> items with minimum value of 0 and maximum value of
        /// <paramref name="maximumValue"/>.
        /// </param>
        /// <param name="itemCount">The number of items in <paramref name="data"/>.</param>
        /// <param name="maximumValue">Maximum value in <paramref name="data"/>.</param>
        /// <param name="writer">
        /// Destination of <paramref name="data"/>'s bit-packed representation.
        /// </param>
        public static void Pack(
            IEnumerable<ulong> data,
            int itemCount,
            ulong maximumValue,
            ref ByteWriter writer)
        {
            var sizeInfo = PackSizeAdvanced(itemCount, maximumValue);
            var packed = writer.VirtualByteSpanForward(sizeInfo.TotalBytes);
            var currentBitPosition = 0;

            foreach (var value in data)
            {
                // Calculate which byte(s) this value's bits will go into
                var startByteIndex = currentBitPosition / 8;
                var bitOffsetInStartByte = currentBitPosition % 8;
                // Write the value's bits into the byte array
                var remainingBits = sizeInfo.BitsPerValue;
                var remainingValue = value;

                while (remainingBits > 0)
                {
                    // Calculate how many bits we can write to the current byte
                    var bitsToWrite = Math.Min(8 - bitOffsetInStartByte, (int)remainingBits);

                    // Extract the bits we want to write
                    // Handle the case where we need all 64 bits (for ulong.MaxValue)
                    var mask = bitsToWrite == 64 ? ulong.MaxValue : (1UL << bitsToWrite) - 1;
                    var bits = (byte)(remainingValue & mask);

                    // Shift the bits to their correct position in the byte
                    bits = (byte)(bits << bitOffsetInStartByte);

                    // Combine with existing bits in the byte using OR
                    packed[startByteIndex] |= bits;

                    // Update our position trackers
                    remainingValue >>= bitsToWrite;
                    remainingBits -= bitsToWrite;
                    startByteIndex++;
                    bitOffsetInStartByte = 0;
                }

                currentBitPosition += sizeInfo.BitsPerValue;
            }
        }

        /// <summary>
        /// Unpacks a sequence of bit-packed values back into their original long values.
        /// </summary>
        /// <param name="packed">The byte array containing bit-packed values.</param>
        /// <param name="itemCount">The number of values to unpack.</param>
        /// <param name="maximumValue">The maximum possible value in the original data.</param>
        /// <returns>Array of unpacked ulong values.</returns>
        public static UnpackedValues Unpack(
            ReadOnlySpan<byte> packed,
            int itemCount,
            ulong maximumValue)
        {
            var sizeInfo = PackSizeAdvanced(itemCount, maximumValue);

            if (packed.Length != sizeInfo.TotalBytes)
            {
                throw new ArgumentOutOfRangeException(nameof(packed));
            }

            return new(packed, sizeInfo.BitsPerValue, itemCount);
        }
    }
}

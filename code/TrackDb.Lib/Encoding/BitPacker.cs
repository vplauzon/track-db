using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
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
            if (maximumValue == 0)
            {
                throw new ArgumentOutOfRangeException(nameof(maximumValue));
            }

            //  Calculate number of bits needed per value
            var bitsPerValue = maximumValue == ulong.MaxValue
                ? 64
                : (int)Math.Ceiling(Math.Log2(maximumValue + 1));
            //  Calculate total bits and bytes needed
            var totalBits = (long)itemCount * bitsPerValue;
            var totalBytes = (int)((totalBits + 7) / 8);    // Round up to nearest byte

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
        /// <param name="maximumValue">Maximum value in <paramref name="data"/>.</param>
        /// <param name="writer">
        /// Destination of <paramref name="data"/>'s bit-packed representation.
        /// </param>
        public static void Pack(
            scoped ReadOnlySpan<ulong> data,
            ulong maximumValue,
            ref ByteWriter writer)
        {
            var sizeInfo = PackSizeAdvanced(data.Length, maximumValue);
            var buffer = writer.SliceForward(sizeInfo.TotalBytes);
            var currentBitPosition = 0;

            //  Since we do bit manipulations and will write partial bytes,
            //  we want to ensure we start with a clean slate
            buffer.Fill(0);
            foreach (var value in data)
            {
                // Calculate which byte(s) this value's bits will go into
                var startByteIndex = currentBitPosition / 8;
                var bitOffsetInStartByte = currentBitPosition % 8;
                // Write the value's bits into the byte array
                var remainingBits = sizeInfo.BitsPerValue;
                var remainingValue = value;

                if (value > maximumValue)
                {
                    throw new ArgumentOutOfRangeException(nameof(value));
                }
                while (remainingBits > 0)
                {
                    // Calculate how many bits we can write to the current byte
                    var bitsToWrite = Math.Min(8 - bitOffsetInStartByte, (int)remainingBits);

                    //  Extract the bits we want to write
                    var mask = (1UL << bitsToWrite) - 1;
                    var bits = (byte)(remainingValue & mask);

                    // Shift the bits to their correct position in the byte
                    bits = (byte)(bits << bitOffsetInStartByte);

                    // Combine with existing bits in the byte using OR
                    buffer[startByteIndex] |= bits;

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
        /// Unpacks a sequence of bit-packed values back into their original ulong values.
        /// </summary>
        /// <param name="packed">The byte array containing bit-packed values.</param>
        /// <param name="maximumValue">The maximum possible value in the original data.</param>
        /// <param name="values">The unpacked values.</param>
        public static void Unpack(
            ReadOnlySpan<byte> packed,
            ulong maximumValue,
            Span<ulong> values)
        {
            var sizeInfo = PackSizeAdvanced(values.Length, maximumValue);
            var currentBitPosition = 0;

            if (packed.Length != sizeInfo.TotalBytes)
            {
                throw new ArgumentOutOfRangeException(nameof(packed));
            }

            for (int i = 0; i != values.Length; ++i)
            {
                var startByteIndex = currentBitPosition / 8;
                var bitOffsetInStartByte = currentBitPosition % 8;
                var remainingBits = sizeInfo.BitsPerValue;
                var value = (ulong)0;
                var bitsProcessed = 0;

                while (remainingBits > 0)
                {
                    // Calculate how many bits we can read from current byte
                    var bitsToRead = Math.Min(8 - bitOffsetInStartByte, remainingBits);
                    // Extract bits from current byte
                    var currentByte = packed[startByteIndex];
                    var mask = (1UL << bitsToRead) - 1;
                    var bits = (currentByte >> bitOffsetInStartByte) & (byte)mask;

                    // Add these bits to our value (ensure 64-bit operations throughout)
                    value |= ((ulong)bits << bitsProcessed);

                    // Update our trackers
                    remainingBits -= bitsToRead;
                    bitsProcessed += bitsToRead;
                    startByteIndex++;
                    bitOffsetInStartByte = 0;
                }
                if (value > maximumValue)
                {
                    throw new ArgumentOutOfRangeException(
                        $"Unpacked value ({value}) > maximum value ({maximumValue})");
                }
                currentBitPosition += sizeInfo.BitsPerValue;
                values[i] = value;
            }
        }
    }
}

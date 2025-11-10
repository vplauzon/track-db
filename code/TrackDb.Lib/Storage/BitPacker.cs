using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib.Storage
{
    /// <summary>
    /// Packs / unpacks <see cref="ulong"/> array in bytes by leveraging the real number of bytes
    /// needed given the range (min/max) of integers in the array.
    /// </summary>
    internal static class BitPacker
    {
        /// <summary>
        /// Packs a sequence of <see cref="ulong"/>s.  Each item is encoded into a sequence of bits.
        /// Number of bits per item is dictated by the range of value, which is 0 to
        /// <paramref name="maximumValue"/>.
        /// </summary>
        /// <param name="data">
        /// Sequence of <see cref="ulong"/> items with minimum value of 0 and maximum value of
        /// <paramref name="maximumValue"/>.
        /// </param>
        /// <param name="dataCount">The number of items in <paramref name="data"/>.</param>
        /// <param name="maximumValue">Maximum value in <paramref name="data"/>.</param>
        /// <returns>Byte array of bit-packed representation of <paramref name="data"/>.</returns>
        public static byte[] Pack(IEnumerable<ulong> data, int dataCount, ulong maximumValue)
        {
            // Calculate number of bits needed per value
            var bitsPerValue = maximumValue == ulong.MaxValue 
                ? 64 
                : (int)Math.Ceiling(Math.Log2(maximumValue + 1));
            // Calculate total bits and bytes needed
            var totalBits = dataCount * bitsPerValue;
            var totalBytes = (totalBits + 7) / 8; // Round up to nearest byte
            var result = new byte[totalBytes];
            var currentBitPosition = 0;
            
            foreach (var value in data)
            {
                // Calculate which byte(s) this value's bits will go into
                var startByteIndex = currentBitPosition / 8;
                var bitOffsetInStartByte = currentBitPosition % 8;
                // Write the value's bits into the byte array
                var remainingBits = bitsPerValue;
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
                    result[startByteIndex] |= bits;
                    
                    // Update our position trackers
                    remainingValue >>= bitsToWrite;
                    remainingBits -= bitsToWrite;
                    startByteIndex++;
                    bitOffsetInStartByte = 0;
                }
                
                currentBitPosition += bitsPerValue;
            }
            
            return result;
        }

        /// <summary>
        /// Unpacks a sequence of bit-packed values back into their original long values.
        /// </summary>
        /// <param name="data">The byte array containing bit-packed values.</param>
        /// <param name="dataCount">The number of values to unpack.</param>
        /// <param name="maximumValue">The maximum possible value in the original data.</param>
        /// <returns>Array of unpacked ulong values.</returns>
        public static ulong[] Unpack(ReadOnlySpan<byte> data, int dataCount, ulong maximumValue)
        {
            // Calculate number of bits per value (same as Pack method)
            var bitsPerValue = maximumValue == ulong.MaxValue 
                ? 64 
                : (int)Math.Ceiling(Math.Log2(maximumValue + 1));
            var result = new ulong[dataCount];
            var currentBitPosition = 0;
            
            for (var i = 0; i < dataCount; i++)
            {
                // Calculate which byte we start reading from
                var startByteIndex = currentBitPosition / 8;
                var bitOffsetInStartByte = currentBitPosition % 8;
                var remainingBits = bitsPerValue;
                var value = 0UL;
                var bitsProcessed = 0;
                
                while (remainingBits > 0)
                {
                    // Calculate how many bits we can read from current byte
                    var bitsToRead = Math.Min(8 - bitOffsetInStartByte, remainingBits);
                    
                    // Extract bits from current byte
                    var currentByte = data[startByteIndex];
                    var mask = bitsToRead == 64 ? ulong.MaxValue : (1UL << bitsToRead) - 1;
                    var bits = (currentByte >> bitOffsetInStartByte) & (byte)mask;
                    
                    // Add these bits to our value (ensure 64-bit operations throughout)
                    value |= ((ulong)bits << bitsProcessed);
                    
                    // Update our trackers
                    remainingBits -= bitsToRead;
                    bitsProcessed += bitsToRead;
                    startByteIndex++;
                    bitOffsetInStartByte = 0;
                }
                
                result[i] = value;
                currentBitPosition += bitsPerValue;
            }
            
            return result;
        }
    }
}

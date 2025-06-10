using System;
using System.Collections.Immutable;

namespace Ipdb.Lib
{
    internal record IndexDefinition<T>(
        Func<T, IndexValues> ObjectExtractor,
        IImmutableList<IndexType> IndexTypes)
    {
        public static IndexDefinition<T> CreateIndex<PT>(Func<T, PT> propertyExtractor)
        {
            if (typeof(PT) == typeof(int))
            {
                return new IndexDefinition<T>(
                    o =>
                    {
                        throw new NotSupportedException();
                    },
                    ImmutableArray.Create(IndexType.Int));
            }
            else
            {
                throw new NotSupportedException($"Type '{typeof(PT).Name}' for index");
            }
        }

        #region Object Extractor
        private static Func<T, IndexValues> GetIntObjectExtractor(Func<T, int> propertyExtractor)
        {
            return o =>
            {
                var value = propertyExtractor(o);
                // XOR the upper and lower 16 bits of the int
                var hash = (short)((value & 0xFFFF) ^ ((value >> 16) & 0xFFFF));

                return new IndexValues(value, hash);
            };
        }

        private static short GetHash(long value)
        {
            // XOR all four 16-bit components of the long
            return (short)(
                (value & 0xFFFF) ^
                ((value >> 16) & 0xFFFF) ^
                ((value >> 32) & 0xFFFF) ^
                ((value >> 48) & 0xFFFF));
        }
        #endregion
    }
}
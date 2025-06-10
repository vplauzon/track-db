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
            return new IndexDefinition<T>(
                o =>
                {
                    var value = propertyExtractor(o);

                    throw new NotSupportedException();
                    //return new IndexValues(value, GetHash(value));
                },
                ImmutableArray.Create(IndexType.Enum));
        }

        #region Get Hash methods
        private static short GetHash<TEnum>(TEnum value) where TEnum : Enum
        {
            throw new NotImplementedException();
        }

        private static short GetHash(string value)
        {
            throw new NotImplementedException();
        }

        private static short GetHash(int value)
        {
            // XOR the upper and lower 16 bits of the int
            return (short)((value & 0xFFFF) ^ ((value >> 16) & 0xFFFF));
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
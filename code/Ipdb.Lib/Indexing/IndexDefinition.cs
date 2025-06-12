using System;
using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;

namespace Ipdb.Lib
{
    internal record IndexDefinition<T>(
        Func<T, IndexValues> ObjectExtractor,
        Expression PropertyExpression,
        IImmutableList<IndexType> IndexTypes)
    {
        #region Constructors
        public static IndexDefinition<T> CreateIndex<PT>(
            Expression<Func<T, PT>> propertyExtractor)
        {
            if (typeof(PT) == typeof(int))
            {
                // Get the method info for GetIntObjectExtractor
                const string METHOD_NAME = "GetIntObjectExtractor";
                
                var method = typeof(IndexDefinition<T>).GetMethod(
                    METHOD_NAME,
                    BindingFlags.NonPublic | BindingFlags.Static) 
                    ?? throw new InvalidOperationException($"Method {METHOD_NAME} not found");
                // Invoke the method to get our object extractor
                var objectExtractor = (Func<T, IndexValues>?)method.Invoke(
                    null,
                    [propertyExtractor.Compile()])
                    ?? throw new InvalidOperationException("Failed to create object extractor");

                return new IndexDefinition<T>(
                    objectExtractor,
                    propertyExtractor,
                    ImmutableArray.Create(IndexType.Int));
            }
            else
            {
                throw new NotSupportedException($"Type '{typeof(PT).Name}' for index");
            }
        }

        #region Object Extractor
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.NonPublicMethods)]
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
        #endregion

        public bool IsIndexUsed<PT>(Expression<Func<T, PT>> propertyExtractor)
        {
            throw new NotImplementedException();
        }
    }
}
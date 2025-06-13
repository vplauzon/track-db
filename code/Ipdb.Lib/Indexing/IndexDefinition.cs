using Ipdb.Lib.Indexing;
using System;
using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;

namespace Ipdb.Lib
{
    internal record IndexDefinition<T>(
        string PropertyPath,
        Func<T, object?> KeyExtractor,
        Func<T, short> HashExtractor,
        object HashFunc)
    {
        #region Constructors
        public static IndexDefinition<T> CreateIndex<PT>(
            Expression<Func<T, PT>> propertyExtractor)
        {
            var path = propertyExtractor.ToPath();
            //  Compile into a delegate
            var keyExtractor = propertyExtractor.Compile()
                ?? throw new InvalidOperationException(
                    $"Can't compile property extractor for '{path}'");
            var objectKeyExtractor = (T document) => (object?)keyExtractor(document);

            if (typeof(PT) == typeof(int))
            {
                const string METHOD_NAME = "GetIntHashPair";

                var hashFromObjectMethod = typeof(IndexDefinition<T>).GetMethod(
                    METHOD_NAME,
                    BindingFlags.NonPublic | BindingFlags.Static)
                    ?? throw new InvalidOperationException($"Method {METHOD_NAME} not found");
                //  Invoke the method to get our object extractor
                var funcPair = ((Func<T, short>, Func<PT, short>)?)hashFromObjectMethod.Invoke(
                    null,
                    [keyExtractor])
                    ?? throw new InvalidOperationException("Failed to create object extractor");

                return new IndexDefinition<T>(
                    path,
                    objectKeyExtractor,
                    funcPair.Item1,
                    funcPair.Item2);
            }
            else
            {
                throw new NotSupportedException($"Type '{typeof(PT).Name}' for index");
            }
        }

        #region Object Extractor
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.NonPublicProperties)]
        private static (Func<T, short>, Func<int, short>) GetIntHashPair(Func<T, int> propertyExtractor)
        {
            return (t =>
            {
                var property = propertyExtractor(t);
                var hash = GetIntHash(property);

                return hash;
            },
            GetIntHash);
        }
        #endregion

        #region Hash methods
        private static short GetIntHash(int keyValue)
        {
            // XOR the upper and lower 16 bits of the int
            var hash = (short)((keyValue & 0xFFFF) ^ ((keyValue >> 16) & 0xFFFF));

            return hash;
        }

        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.NonPublicProperties)]
        private static short GetLongHash(long value)
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
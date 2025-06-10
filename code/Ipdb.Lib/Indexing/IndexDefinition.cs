using System;
using System.Collections.Immutable;

namespace Ipdb.Lib
{
    internal record IndexDefinition<T>(
        Func<T, object> objectExtractor,
        IImmutableList<IndexType> IndexTypes)
    {
        public static IndexDefinition<T> CreateIndex<PT>(Func<T, PT> propertyExtractor)
            where PT : notnull
        {
            return new IndexDefinition<T>(
                o => propertyExtractor(o),
                GetIndexTypes<PT>());
        }

        private static IImmutableList<IndexType> GetIndexTypes<PT>()
        {
            var type = typeof(PT);

            if (type.IsEnum)
            {
                return ImmutableArray.Create(IndexType.Enum);
            }
            if (type == typeof(string))
            {
                return ImmutableArray.Create(IndexType.String);
            }
            if (type == typeof(int))
            {
                return ImmutableArray.Create(IndexType.Int);
            }
            if (type == typeof(long))
            {
                return ImmutableArray.Create(IndexType.Long);
            }

            throw new ArgumentException(
                $"Type {type.Name} is not supported as an index type. " +
                "Supported types are: enum, string, int, and long.");
        }
    }
}
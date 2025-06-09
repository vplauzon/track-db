using System;
using System.Collections.Immutable;

namespace Ipdb.Lib
{
    /// <summary>
    /// Table schema including a primary index (mandatory)
    /// and optionally one or many secondary indexes.
    /// An index can be on a property of type Enum, string, int or long or
    /// a tuple mixing those.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class TableSchema<T>
    {
        #region Inner Types
        private enum IndexType
        {
            Enum,
            String,
            Int,
            Long
        }

        private record Index(
            Func<T, object> objectExtractor,
            IImmutableList<IndexType> IndexTypes)
        {
            public static Index CreateIndex<PT>(Func<T, PT> propertyExtractor)
                where PT : notnull
            {
                return new Index(
                    o => propertyExtractor(o),
                    GetIndexTypes<PT>());
            }
        }
        #endregion

        private readonly Index _primaryIndex;
        private readonly IImmutableList<Index> _secondaryIndexes;

        #region Constructors
        public static TableSchema<T> CreateSchema<PT>(Func<T, PT> propertyExtractor)
            where PT : notnull
        {
            return new TableSchema<T>(
                Index.CreateIndex(propertyExtractor),
                ImmutableArray<Index>.Empty);
        }

        private TableSchema(
            Index primaryIndex,
            IImmutableList<Index> secondaryIndexes)
        {
            _primaryIndex = primaryIndex;
            _secondaryIndexes = secondaryIndexes;
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
        #endregion

        public TableSchema<T> AddSecondaryIndex<PT>(Func<T, PT> propertyExtractor)
            where PT : notnull
        {
            return new TableSchema<T>(
                _primaryIndex,
                _secondaryIndexes.Add(
                    Index.CreateIndex(propertyExtractor)));
        }
    }
}

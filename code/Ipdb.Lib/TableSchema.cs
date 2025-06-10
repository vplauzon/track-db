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
    public partial class TableSchema<T>
    {
        private readonly IndexDefinition _primaryIndex;
        private readonly IImmutableList<IndexDefinition> _secondaryIndexes;

        #region Constructors
        public static TableSchema<T> CreateSchema<PT>(Func<T, PT> primaryIndexExtractor)
            where PT : notnull
        {
            return new TableSchema<T>(
                IndexDefinition.CreateIndex(primaryIndexExtractor),
                ImmutableArray<IndexDefinition>.Empty);
        }

        private TableSchema(
            IndexDefinition primaryIndex,
            IImmutableList<IndexDefinition> secondaryIndexes)
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
                    IndexDefinition.CreateIndex(propertyExtractor)));
        }
    }
}

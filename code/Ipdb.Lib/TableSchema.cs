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
        private readonly IndexDefinition<T> _primaryIndex;
        private readonly IImmutableList<IndexDefinition<T>> _secondaryIndexes;

        #region Constructors
        public static TableSchema<T> CreateSchema<PT>(Func<T, PT> primaryIndexExtractor)
            where PT : notnull
        {
            return new TableSchema<T>(
                IndexDefinition<T>.CreateIndex(primaryIndexExtractor),
                ImmutableArray<IndexDefinition<T>>.Empty);
        }

        private TableSchema(
            IndexDefinition<T> primaryIndex,
            IImmutableList<IndexDefinition<T>> secondaryIndexes)
        {
            _primaryIndex = primaryIndex;
            _secondaryIndexes = secondaryIndexes;
        }
        #endregion

        public TableSchema<T> AddSecondaryIndex<PT>(Func<T, PT> propertyExtractor)
            where PT : notnull
        {
            return new TableSchema<T>(
                _primaryIndex,
                _secondaryIndexes.Add(
                    IndexDefinition<T>.CreateIndex(propertyExtractor)));
        }
    }
}

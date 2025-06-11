using System;
using System.Collections.Immutable;
using System.Linq.Expressions;

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
        #region Constructors
        public static TableSchema<T> CreateSchema<PT>(
            Expression<Func<T, PT>> primaryIndexExtractor)
            where PT : notnull
        {
            return new TableSchema<T>(
                IndexDefinition<T>.CreateIndex(primaryIndexExtractor),
                ImmutableArray<IndexDefinition<T>>.Empty);
        }

        public TableSchema<T> AddSecondaryIndex<PT>(Expression<Func<T, PT>> propertyExtractor)
            where PT : notnull
        {
            return new TableSchema<T>(
                PrimaryIndex,
                SecondaryIndexes.Add(
                    IndexDefinition<T>.CreateIndex(propertyExtractor)));
        }

        private TableSchema(
            IndexDefinition<T> primaryIndex,
            IImmutableList<IndexDefinition<T>> secondaryIndexes)
        {
            PrimaryIndex = primaryIndex;
            SecondaryIndexes = secondaryIndexes;
        }
        #endregion

        internal IndexDefinition<T> PrimaryIndex { get; }

        internal IImmutableList<IndexDefinition<T>> SecondaryIndexes { get; }
    }
}

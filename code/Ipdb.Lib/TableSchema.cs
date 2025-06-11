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
                ImmutableArray.Create(IndexDefinition<T>.CreateIndex(primaryIndexExtractor)));
        }

        public TableSchema<T> AddSecondaryIndex<PT>(Expression<Func<T, PT>> propertyExtractor)
            where PT : notnull
        {
            return new TableSchema<T>(
                Indexes.Add(
                    IndexDefinition<T>.CreateIndex(propertyExtractor)));
        }

        private TableSchema(IImmutableList<IndexDefinition<T>> indexes)
        {
            Indexes = indexes;
        }
        #endregion

        internal IImmutableList<IndexDefinition<T>> Indexes { get; }
    }
}

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
            string tableName,
            Expression<Func<T, PT>> primaryIndexExtractor)
            where PT : notnull
        {
            return new TableSchema<T>(
                tableName,
                ImmutableArray.Create(IndexDefinition<T>.CreateIndex(primaryIndexExtractor)));
        }

        public TableSchema<T> AddSecondaryIndex<PT>(Expression<Func<T, PT>> propertyExtractor)
            where PT : notnull
        {
            return new TableSchema<T>(
                TableName,
                Indexes.Add(
                    IndexDefinition<T>.CreateIndex(propertyExtractor)));
        }

        private TableSchema(string tableName, IImmutableList<IndexDefinition<T>> indexes)
        {
            TableName = tableName;
            Indexes = indexes;
        }
        #endregion

        public string TableName { get; }

        internal IImmutableList<IndexDefinition<T>> Indexes { get; }
    }
}

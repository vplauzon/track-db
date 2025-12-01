using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using TrackDb.Lib.InMemory.Block;

namespace TrackDb.Lib
{
    /// <summary>
    /// Represents the schema of a table.
    /// </summary>
    public class TableSchema
    {
        private readonly IImmutableDictionary<string, int> _columnNameToColumnIndexMap;

        internal protected const string CREATION_TIME = "$creationTime";
        internal protected const string RECORD_ID = "$recordId";

        public TableSchema(
            string tableName,
            IEnumerable<ColumnSchema> columns,
            IEnumerable<int> primaryKeyColumnIndexes,
            IEnumerable<int> partitionKeyColumnIndexes)
            : this(
                  tableName,
                  columns.Select(c => new ColumnSchemaProperties(c, ColumnSchemaStat.Data)),
                  primaryKeyColumnIndexes.ToImmutableArray(),
                  partitionKeyColumnIndexes.ToImmutableArray(),
                  true)
        {
        }

        internal TableSchema(
            string tableName,
            IEnumerable<ColumnSchemaProperties> columnProperties,
            IEnumerable<int> primaryKeyColumnIndexes,
            IEnumerable<int> partitionKeyColumnIndexes,
            bool areExtraColumnsIndexed)
        {
            //  Validate column types
            var unsupportedColumns = columnProperties
                .Select(c => c.ColumnSchema)
                .Where(c => !ReadOnlyBlockBase.IsSupportedDataColumnType(c.ColumnType));

            if (unsupportedColumns.Any())
            {
                var unsupportedColumn = unsupportedColumns.First();

                throw new NotSupportedException(
                    $"Column type '{unsupportedColumn.ColumnType.Name}' on column " +
                    $"'{unsupportedColumn.ColumnName}'");
            }

            //  Validate column name duplicates
            var firstDuplicatedColumnName = columnProperties
                .Select(c => c.ColumnSchema)
                .GroupBy(c => c.ColumnName)
                .Where(g => g.Count() > 1)
                .Select(g => g.Key)
                .FirstOrDefault();

            if (firstDuplicatedColumnName != null)
            {
                throw new ArgumentException(
                    nameof(columnProperties),
                    $"Duplicated column name:  '{firstDuplicatedColumnName}'");
            }

            //  Validate Partition key column indexes
            var outOfRangePartitionKeyColumnIndexes = partitionKeyColumnIndexes
                .Where(i => i < 0 || i >= columnProperties.Count());

            if (outOfRangePartitionKeyColumnIndexes.Any())
            {
                var outOfRangePartitionKeyColumnIndex =
                    outOfRangePartitionKeyColumnIndexes.First();

                throw new ArgumentOutOfRangeException(
                    nameof(partitionKeyColumnIndexes),
                    $"Column index '{outOfRangePartitionKeyColumnIndex}'");
            }

            TableName = tableName;
            Columns = columnProperties.Select(c => c.ColumnSchema).ToImmutableList();
            ColumnProperties = columnProperties
                .Append(new(new(CREATION_TIME, typeof(DateTime), areExtraColumnsIndexed), ColumnSchemaStat.Data))
                .Append(new(new(RECORD_ID, typeof(long), areExtraColumnsIndexed), ColumnSchemaStat.Data))
                .ToImmutableArray();
            PrimaryKeyColumnIndexes = primaryKeyColumnIndexes.ToImmutableArray();
            PartitionKeyColumnIndexes = partitionKeyColumnIndexes.ToImmutableArray();
            _columnNameToColumnIndexMap = Columns
                .Index()
                .ToImmutableDictionary(o => o.Item.ColumnName, o => o.Index);
        }

        public string TableName { get; }

        public IImmutableList<ColumnSchema> Columns { get; }

        internal IImmutableList<ColumnSchemaProperties> ColumnProperties { get; }

        #region Extra columns
        public int CreationTimeColumnIndex => Columns.Count;

        public int RecordIdColumnIndex => Columns.Count + 1;

        public int RowIndexColumnIndex => Columns.Count + 2;

        public int ParentBlockIdColumnIndex => Columns.Count + 3;
        #endregion

        /// <summary>
        /// Primary keys are used in
        /// <see cref="Table.UpdateRecord(ReadOnlySpan{object?}, ReadOnlySpan{object?}, TransactionContext?)"/>.
        /// It is used to delete an old record by doing a where-clause on its primary key.
        /// </summary>
        public IImmutableList<int> PrimaryKeyColumnIndexes { get; }

        public IImmutableList<int> PartitionKeyColumnIndexes { get; }

        public int FindColumnIndex(string columnName)
        {
            var index = Columns
                .Index()
                .Where(b => b.Item.ColumnName == columnName)
                .Select(b => (int?)b.Index)
                .FirstOrDefault();

            return index ?? throw new ArgumentOutOfRangeException(
                nameof(columnName),
                $"Column '{columnName}' not found");
        }

        internal bool AreColumnsCompatible(IImmutableList<ColumnSchema> otherColumns)
        {
            return otherColumns.Count == Columns.Count
                && Columns.Zip(otherColumns)
                .All(b => b.First.ColumnType == b.Second.ColumnType);
        }

        internal bool TryGetColumnIndex(string columnName, out int columnIndex)
        {
            return _columnNameToColumnIndexMap.TryGetValue(columnName, out columnIndex);
        }

        internal MetadataTableSchema CreateMetadataTableSchema()
        {
            return MetadataTableSchema.FromParentSchema(this);
        }
    }
}
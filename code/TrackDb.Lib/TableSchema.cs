using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using TrackDb.Lib.InMemory.Block;

namespace TrackDb.Lib
{
    /// <summary>Represents the schema of a table.</summary>
    public abstract class TableSchema
    {
        private readonly IImmutableDictionary<string, int> _columnNameToColumnIndexMap;

        internal TableSchema(
            string tableName,
            IEnumerable<ColumnSchemaProperties> columnProperties,
            IEnumerable<ColumnSchemaProperties> extraColumnProperties,
            IEnumerable<int> primaryKeyColumnIndexes,
            IEnumerable<int> partitionKeyColumnIndexes,
            IEnumerable<TableTriggerAction> triggerActions)
        {
            var columns = columnProperties
                .Select(c => c.ColumnSchema)
                .ToImmutableArray();
            var allColumnProperties = columnProperties
                .Concat(extraColumnProperties)
                .ToImmutableArray();

            //  Validate column types
            var unsupportedColumns = allColumnProperties
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
            var firstDuplicatedColumnName = allColumnProperties
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
                .Where(i => i < 0 || i >= columns.Length);

            if (outOfRangePartitionKeyColumnIndexes.Any())
            {
                var outOfRangePartitionKeyColumnIndex =
                    outOfRangePartitionKeyColumnIndexes.First();

                throw new ArgumentOutOfRangeException(
                    nameof(partitionKeyColumnIndexes),
                    $"Column index '{outOfRangePartitionKeyColumnIndex}'");
            }

            TableName = tableName;
            Columns = columns;
            ColumnProperties = allColumnProperties;
            PrimaryKeyColumnIndexes = primaryKeyColumnIndexes.ToImmutableArray();
            PartitionKeyColumnIndexes = partitionKeyColumnIndexes.ToImmutableArray();
            TriggerActions = triggerActions.ToImmutableArray();
            _columnNameToColumnIndexMap = Columns
                .Index()
                .ToImmutableDictionary(o => o.Item.ColumnName, o => o.Index);
        }

        public string TableName { get; }

        public IImmutableList<ColumnSchema> Columns { get; }

        internal IImmutableList<ColumnSchemaProperties> ColumnProperties { get; }

        internal abstract bool IsMetadata { get; }

        /// <summary>
        /// Primary keys are used in
        /// <see cref="Table.UpdateRecord(ReadOnlySpan{object?}, ReadOnlySpan{object?}, TransactionContext?)"/>.
        /// It is used to delete an old record by doing a where-clause on its primary key.
        /// </summary>
        public IImmutableList<int> PrimaryKeyColumnIndexes { get; }

        public IImmutableList<int> PartitionKeyColumnIndexes { get; }

        public IImmutableList<TableTriggerAction> TriggerActions { get; }

        public int FindColumnIndex(string columnName)
        {
            if (TryGetColumnIndex(columnName, out int columnIndex))
            {
                return columnIndex;
            }
            else
            {
                throw new ArgumentOutOfRangeException(
                    nameof(columnName),
                    $"Column '{columnName}' not found");
            }
        }

        public override string ToString()
        {
            return $"'{TableName}' ({ColumnProperties.Count})";
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
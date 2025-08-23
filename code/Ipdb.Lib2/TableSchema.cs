using Ipdb.Lib2.Cache.CachedBlock;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace Ipdb.Lib2
{
    /// <summary>
    /// Represents the schema of a table.
    /// </summary>
    public class TableSchema
    {
        private readonly IImmutableDictionary<string, int> _columnNameToColumnIndexMap;

        public TableSchema(
            string tableName,
            IEnumerable<ColumnSchema> columns,
            IEnumerable<int> partitionKeyColumnIndexes)
        {
            //  Validate column types
            var unsupportedColumns = columns
                .Where(c => !ReadOnlyBlockBase.SupportedDataColumnTypes.Contains(c.ColumnType));

            if (unsupportedColumns.Any())
            {
                var unsupportedColumn = unsupportedColumns.First();

                throw new NotSupportedException(
                    $"Column type '{unsupportedColumn.ColumnType.Name}' on column " +
                    $"'{unsupportedColumn.ColumnName}'");
            }

            //  Validate column name duplicates
            var firstDuplicatedColumnName = columns
                .GroupBy(c => c.ColumnName)
                .Where(g => g.Count() > 1)
                .Select(g => g.Key)
                .FirstOrDefault();

            if (firstDuplicatedColumnName != null)
            {
                throw new ArgumentException(
                    nameof(columns),
                    $"Duplicated column name:  '{firstDuplicatedColumnName}'");
            }

            //  Validate Partition key column indexes
            var outOfRangePartitionKeyColumnIndexes = partitionKeyColumnIndexes
                .Where(i => i < 0 || i >= columns.Count());

            if (outOfRangePartitionKeyColumnIndexes.Any())
            {
                var outOfRangePartitionKeyColumnIndex =
                    outOfRangePartitionKeyColumnIndexes.First();

                throw new ArgumentOutOfRangeException(
                    nameof(partitionKeyColumnIndexes),
                    $"Column index '{outOfRangePartitionKeyColumnIndex}'");
            }

            TableName = tableName;
            Columns = columns.ToImmutableList();
            PartitionKeyColumnIndexes = partitionKeyColumnIndexes.ToImmutableArray();
            _columnNameToColumnIndexMap = Columns
                .Index()
                .ToImmutableDictionary(o => o.Item.ColumnName, o => o.Index);
        }

        public string TableName { get; }

        public IImmutableList<ColumnSchema> Columns { get; }

        public IImmutableList<int> PartitionKeyColumnIndexes { get; }

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
    }
}
using Ipdb.Lib2.Query;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Reflection;

namespace Ipdb.Lib2.Cache.CachedBlock
{
    internal class BlockBuilder : IBlock
    {
        private readonly TableSchema _schema;
        private readonly ICachedColumn _recordIdColumn;
        private readonly IImmutableList<ICachedColumn> _dataColumns;
        private readonly object[] _dataColumnBuffer;

        #region Constructors
        public BlockBuilder(TableSchema schema)
        {
            _schema = schema;
            _recordIdColumn = CreateRecordIdColumn(Array.Empty<long>());
            _dataColumns = _schema.Columns
                .Select(c => CreateCachedColumn(c.ColumnType, Array.Empty<object>()))
                .ToImmutableArray();
            _dataColumnBuffer = new object[_schema.Columns.Count];
        }

        public BlockBuilder(IBlock block)
        {
            _schema = block.TableSchema;
            _recordIdColumn = CreateRecordIdColumn(block.RecordIds);
            _dataColumns = Enumerable.Range(0, _schema.Columns.Count)
                .Select(i => CreateCachedColumn(
                    _schema.Columns[i].ColumnType,
                    block.GetColumnData(i)))
                .ToImmutableArray();
            _dataColumnBuffer = new object[_schema.Columns.Count];
        }

        private static ICachedColumn CreateCachedColumn(
            Type columnType,
            IEnumerable<object> data)
        {
            var cachedColumnType = typeof(SimpleCachedColumn<>).MakeGenericType(columnType);
            var cachedColumn = Activator.CreateInstance(
                cachedColumnType,
                BindingFlags.Instance | BindingFlags.Public,
                null,
                [data],
                null);

            return (ICachedColumn)cachedColumn!;
        }

        private static ICachedColumn CreateRecordIdColumn(IEnumerable<long> recordIds)
        {
            return new SimpleCachedColumn<long>(recordIds.Cast<object>());
        }
        #endregion

        public bool IsEmpty => throw new NotImplementedException();

        public void AppendRecord(long recordId, object record)
        {
            _recordIdColumn.AppendValue(recordId);
            _schema.FromObjectToColumns(record, _dataColumnBuffer);
        }

        #region IBlock
        TableSchema IBlock.TableSchema => _schema;

        int IBlock.RecordCount => _recordIdColumn.RecordCount;

        IEnumerable<long> IBlock.RecordIds => _recordIdColumn.Data.Cast<long>();

        IEnumerable<object> IBlock.GetColumnData(int columnIndex)
        {
            return _dataColumns[columnIndex].Data;
        }

        IImmutableList<long> IBlock.Query(IQueryPredicate predicate, int? takeCount)
        {
            throw new NotImplementedException();
        }
        #endregion
    }
}
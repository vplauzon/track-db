using System;
using System.Collections.Immutable;
using System.Linq;
using System.Reflection;

namespace Ipdb.Lib2.Cache.CachedBlock
{
    internal class BlockBuilder
    {
        private readonly TableSchema _schema;
        private readonly IImmutableList<ICachedColumn> _columns;
        private readonly object[] _columnBuffer;

        #region Constructors
        public BlockBuilder(TableSchema schema)
        {
            _schema = schema;
            _columns = _schema.Columns
                .Select(c => CreateCachedColumn(c.ColumnType))
                .Append(CreateRecordIdColumn())
                .ToImmutableArray();
            _columnBuffer = new object[_schema.Columns.Count];
        }

        private static ICachedColumn CreateCachedColumn(Type columnType)
        {
            var cachedColumnType = typeof(SimpleCachedColumn<>).MakeGenericType(columnType);
            var cachedColumn = Activator.CreateInstance(
                cachedColumnType,
                BindingFlags.Instance | BindingFlags.Public,
                null,
                null,
                null);

            return (ICachedColumn)cachedColumn!;
        }

        private static ICachedColumn CreateRecordIdColumn()
        {
            return new SimpleCachedColumn<long>();
        }
        #endregion

        public bool IsEmpty => throw new NotImplementedException();

        public void AddRecord(long recordId, object record)
        {
            _schema.FromObjectToColumns(record, _columnBuffer);

            throw new NotImplementedException();
        }
    }
}
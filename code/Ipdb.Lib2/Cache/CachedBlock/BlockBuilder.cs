using System;
using System.Collections.Immutable;
using System.Linq;

namespace Ipdb.Lib2.Cache.CachedBlock
{
    internal class BlockBuilder
    {
        private readonly TableSchema _schema;
        private readonly IImmutableList<ICachedColumn> _columns;

        #region Constructors
        public BlockBuilder(TableSchema schema)
        {
            _schema = schema;
            _columns = _schema.Columns
                .Select(c => CreateCachedColumn(c.ColumnType))
                .Append(CreateRecordIdColumn())
                .ToImmutableArray();
        }

        private static ICachedColumn CreateCachedColumn(Type columnType)
        {
            throw new NotImplementedException();
        }

        private static ICachedColumn CreateRecordIdColumn()
        {
            return new SimpleCachedColumn<long>();
        }
        #endregion

        public bool IsEmpty => throw new NotImplementedException();

        public void AddRecord(long recordId, object record)
        {
            throw new NotImplementedException();
        }
    }
}
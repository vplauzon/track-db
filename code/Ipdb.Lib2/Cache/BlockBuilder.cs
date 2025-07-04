using System;
using System.Collections.Immutable;

namespace Ipdb.Lib2.Cache
{
    internal class BlockBuilder
    {
        private readonly TableSchema _schema;

        public BlockBuilder(TableSchema schema)
        {
            _schema = schema;
        }

        public bool IsEmpty => throw new NotImplementedException();

        public void AddRecords(IImmutableList<long> recordIds, IImmutableList<object> records)
        {
            throw new NotImplementedException();
        }
    }
}
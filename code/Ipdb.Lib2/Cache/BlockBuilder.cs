using System;

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

        public void AddRecord(object record)
        {
            throw new NotImplementedException();
        }
    }
}
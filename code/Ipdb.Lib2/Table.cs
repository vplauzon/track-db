using System;
using System.Diagnostics.CodeAnalysis;

namespace Ipdb.Lib2
{
    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.NonPublicConstructors)]
    public class Table<T>
    {
        private readonly TableSchema<T> _schema;

        internal Table(TableSchema<T> schema)
        {
            _schema = schema;
        }

        public void AppendRecord(T record)
        {
            throw new NotImplementedException();
        }
    }
}
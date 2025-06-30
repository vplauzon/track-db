using System;
using System.Diagnostics.CodeAnalysis;
using System.Linq;

namespace Ipdb.Lib2
{
    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.NonPublicConstructors)]
    public class Table<T>
        where T: notnull
    {
        private readonly Database _database;
        private readonly TableSchema<T> _schema;

        internal Table(Database database, TableSchema<T> schema)
        {
            _database = database;
            _schema = schema;
        }

        public void AppendRecord(T record, TransactionContext? transactionContext = null)
        {
            _database.ExecuteWithinTransactionContext(
                transactionContext,
                transactionCache =>
                {
                    if (_schema.PrimaryKeyPropertyPaths.Any())
                    {   //  Delete existing version of the record
                        throw new NotImplementedException();
                    }
                    transactionCache.TransactionLog.AddRecord(record, _schema);
                });
        }
    }
}
using System;
using System.Diagnostics.CodeAnalysis;

namespace Ipdb.Lib2
{
    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.NonPublicConstructors)]
    public class Table<T>
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
                transactionId =>
                {
                    throw new NotImplementedException();
                });
        }
    }
}
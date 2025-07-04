using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using System.Linq;

namespace Ipdb.Lib2
{
    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.NonPublicConstructors)]
    public class Table<T>
        where T : notnull
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
            AppendRecords([record], transactionContext);
        }

        public void AppendRecords(
            IEnumerable<T> records,
            TransactionContext? transactionContext = null)
        {
            _database.ExecuteWithinTransactionContext(
                transactionContext,
                transactionCache =>
                {
                    var objectRecords = records.Cast<object>().ToImmutableArray();
                    var recordIds = _database.NewRecordIds(objectRecords.Length);

                    transactionCache.TransactionLog.AddRecords(
                        recordIds,
                        objectRecords,
                        _schema);
                });
        }
    }
}
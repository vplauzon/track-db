using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace Ipdb.Lib2.Cache
{
    internal record TransactionLog(
        ImmutableDictionary<string, TableTransactionLog>.Builder TableTransactionLogMap)
    {
        public TransactionLog()
            : this(ImmutableDictionary<string, TableTransactionLog>.Empty.ToBuilder())
        {
        }

        public bool IsEmpty => TableTransactionLogMap.Values.All(t => t.IsEmpty);

        public void AppendRecord(long recordId, ReadOnlySpan<object?> record, TableSchema schema)
        {
            if (!TableTransactionLogMap.ContainsKey(schema.TableName))
            {
                TableTransactionLogMap.Add(schema.TableName, new TableTransactionLog(schema));
            }
            TableTransactionLogMap[schema.TableName].AppendRecord(recordId, record);
        }

        public void DeleteRecordIds(IEnumerable<long> recordIds, TableSchema schema)
        {
            if (!TableTransactionLogMap.ContainsKey(schema.TableName))
            {
                TableTransactionLogMap.Add(schema.TableName, new TableTransactionLog(schema));
            }
            TableTransactionLogMap[schema.TableName].DeleteRecordIds(recordIds);
        }
    }
}
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

        public ImmutableTransactionLog ToImmutable()
        {
            throw new NotImplementedException();
        }

        public void AppendRecord(long recordId, object record, TableSchema schema)
        {
            if (!TableTransactionLogMap.ContainsKey(schema.TableName))
            {
                TableTransactionLogMap.Add(schema.TableName, new TableTransactionLog(schema));
            }
            TableTransactionLogMap[schema.TableName].AppendRecord(recordId, record);
        }
    }
}
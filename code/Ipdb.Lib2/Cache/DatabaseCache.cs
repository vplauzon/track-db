using Ipdb.Lib2.DbStorage;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ipdb.Lib2.Cache
{
    internal record DatabaseCache(
        StorageBlockMap StorageBlockMap,
        IImmutableDictionary<string, IImmutableList<ImmutableTableTransactionLog>> TableTransactionLogs)
    {
        public DatabaseCache()
            :this(
                 StorageBlockMap.Empty,
                 ImmutableDictionary<string, IImmutableList<ImmutableTableTransactionLog>>.Empty)
        {
        }

        public DatabaseCache CommitLog(TransactionLog transactionLog)
        {
            var logs = ImmutableDictionary<string, IImmutableList<ImmutableTableTransactionLog>>
                .Empty
                .ToBuilder();

            logs.AddRange(TableTransactionLogs);
            foreach (var pair in transactionLog.TableTransactionLogMap)
            {
                var tableName = pair.Key;
                var newTableTransactionLog = pair.Value;

                if (logs.ContainsKey(tableName))
                {
                    logs[tableName] = logs[tableName].Add(newTableTransactionLog.ToImmutable());
                }
                else
                {
                    logs[tableName] = new[] { newTableTransactionLog.ToImmutable() }.ToImmutableArray();
                }
            }

            return new DatabaseCache(StorageBlockMap, logs.ToImmutableDictionary());
        }
    }
}
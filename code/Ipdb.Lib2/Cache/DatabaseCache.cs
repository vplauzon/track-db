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
        IImmutableDictionary<string, ImmutableTableTransactionLogs> TableTransactionLogsMap)
    {
        public DatabaseCache()
            : this(
                 StorageBlockMap.Empty,
                 ImmutableDictionary<string, ImmutableTableTransactionLogs>.Empty)
        {
        }

        public DatabaseCache CommitLog(TransactionLog transactionLog)
        {
            var logs = ImmutableDictionary<string, ImmutableTableTransactionLogs>
                .Empty
                .ToBuilder();

            logs.AddRange(TableTransactionLogsMap);
            foreach (var pair in transactionLog.TableTransactionLogMap)
            {
                var tableName = pair.Key;
                var newTableTransactionLog = pair.Value;

                if (logs.ContainsKey(tableName))
                {
                    logs[tableName] = new ImmutableTableTransactionLogs(
                        logs[tableName].Logs.Add(newTableTransactionLog.ToImmutable()),
                        logs[tableName].SerializedSize
                        + newTableTransactionLog.BlockBuilder.Serialize().Length);
                }
                else
                {
                    logs[tableName] = new ImmutableTableTransactionLogs(
                        new[] { newTableTransactionLog.ToImmutable() }.ToImmutableArray(),
                        newTableTransactionLog.BlockBuilder.Serialize().Length);
                }
            }

            return new DatabaseCache(StorageBlockMap, logs.ToImmutableDictionary());
        }
    }
}
using Ipdb.Lib2.Cache.CachedBlock;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace Ipdb.Lib2.Cache
{
    internal record TransactionLog(
        ImmutableDictionary<string, BlockBuilder>.Builder TableBlockBuilderMap)
    {
        public TransactionLog()
            : this(ImmutableDictionary<string, BlockBuilder>.Empty.ToBuilder())
        {
        }

        public bool IsEmpty => TableBlockBuilderMap.Values
            .Cast<IBlock>()
            .All(t => t.RecordCount == 0);

        public void AppendRecord(long recordId, ReadOnlySpan<object?> record, TableSchema schema)
        {
            if (!TableBlockBuilderMap.ContainsKey(schema.TableName))
            {
                TableBlockBuilderMap.Add(schema.TableName, new BlockBuilder(schema));
            }
            TableBlockBuilderMap[schema.TableName].AppendRecord(recordId, record);
        }
    }
}
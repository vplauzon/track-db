using Ipdb.Lib2.Cache.CachedBlock;
using System.Collections.Immutable;
using System.Linq;

namespace Ipdb.Lib2.Cache
{
    internal record ImmutableTableTransactionLogs(
        IImmutableList<ImmutableTableTransactionLog> Logs)
    {
        public ImmutableTableTransactionLogs()
            : this(ImmutableArray<ImmutableTableTransactionLog>.Empty)
        {
        }

        public ImmutableTableTransactionLogs MergeLogs()
        {
            if (Logs.Count > 1)
            {
                var newBlock = new BlockBuilder(Logs.First().InMemoryBlock.TableSchema);
                var newDeletedRecordIds = ImmutableHashSet<long>.Empty.ToBuilder();

                foreach (var log in Logs)
                {
                    newBlock.AppendBlock(log.InMemoryBlock);
                    newDeletedRecordIds.UnionWith(log.DeletedRecordIds);
                }

                var newLog = new ImmutableTableTransactionLog(
                    newBlock,
                    newDeletedRecordIds.ToImmutableHashSet());

                return new ImmutableTableTransactionLogs(new[] { newLog }.ToImmutableArray());
            }
            else
            {
                return this;
            }
        }
    }
}
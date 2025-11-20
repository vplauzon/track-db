using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using TrackDb.Lib.InMemory;

namespace TrackDb.Lib.DataLifeCycle
{
    internal class MetaRecordMergeAgent : BlockMergingAgentBase
    {
        public MetaRecordMergeAgent(Database database)
            : base(database)
        {
        }

        public override bool Run(DataManagementActivity forcedDataManagementActivity)
        {
            using (var tx = Database.CreateTransaction())
            {
                var tableMap = Database.GetDatabaseStateSnapshot().TableMap;
                var tableLogs = tx.TransactionState.InMemoryDatabase.TransactionTableLogsMap
                    .Where(p => tableMap[p.Key].IsMetaDataTable);

                if (IsPersistanceRequired(tableLogs.Select(p => p.Value)))
                {
                    var tableCandidates = new Stack<string>(tableLogs
                        .OrderBy(p => p.Value.InMemoryBlocks.Count)
                        .Select(p => p.Key));

                    do
                    {
                        var tableName = tableCandidates.Pop();

                        MergeSubBlocks(tableName, null, tx);
                    }
                    while (IsPersistanceRequired(tableLogs.Select(p => p.Value))
                    && tableCandidates.Any());
                }

                tx.Complete();

                return true;
            }
        }

        private bool IsPersistanceRequired(IEnumerable<ImmutableTableTransactionLogs> tableLogs)
        {
            var totalRecords = tableLogs
                .Sum(logs => logs.InMemoryBlocks.Sum(b => b.RecordCount));

            return totalRecords > Database.DatabasePolicy.InMemoryPolicy.MaxMetaDataRecords;
        }
    }
}
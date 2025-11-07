using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib.SystemData;

namespace TrackDb.Lib.DataLifeCycle
{
    internal class BlockMergingFirstGenerationAgent : BlockMergingAgentBase
    {
        public BlockMergingFirstGenerationAgent(
            Database database,
            TypedTable<TombstoneRecord> tombstoneTable,
            Lazy<DatabaseFileManager> storageManager)
            : base(database, tombstoneTable, storageManager)
        {
        }

        protected override bool RunMerge(bool doMergeAll, bool doPersistMetadata)
        {
            var state = Database.GetDatabaseStateSnapshot();
            var totalRecordCount = state.InMemoryDatabase.TableTransactionLogsMap
                .Where(p => state.TableMap[p.Key].IsMetaDataTable && state.TableMap[p.Key].IsPersisted)
                .SelectMany(p => p.Value.InMemoryBlocks)
                .Sum(b => b.RecordCount);

            if (((doMergeAll || doPersistMetadata) && totalRecordCount > 0)
                || totalRecordCount > Database.DatabasePolicy.InMemoryPolicy.MaxMetaDataRecords)
            {
                return TryMerge(state);
            }
            else
            {
                return true;
            }
        }

        private bool TryMerge(DatabaseState state)
        {
            var metadataTableNames = state.TableMap.Values
                .Where(t => t.IsMetaDataTable && t.IsPersisted)
                .Select(t => t.Table.Schema.TableName)
                .ToImmutableHashSet();
            var metadataBlocks = state.InMemoryDatabase.TableTransactionLogsMap
                .Where(p => metadataTableNames.Contains(p.Key))
                .SelectMany(p => p.Value.InMemoryBlocks);
            var metadataRecords = metadataBlocks
                .Select(b => MetadataRecord.LoadMetaRecords(b))
                .SelectMany(r => r)
                .OrderBy(r => r.Size)
                .ToImmutableArray();

            for (var i = 0; i != metadataRecords.Length; ++i)
            {
                var metadataRecord = metadataRecords[i];
                var neighbours = metadataRecords
                    .Skip(i + 1)
                    .Where(r => r.metadataTableName == metadataRecord.metadataTableName);
                var isMergeSuccessfull = MergeBlocks(neighbours, metadataRecord, false);
                
                if(isMergeSuccessfull)
                {
                    return false;
                }
            }

            return true;
        }
    }
}
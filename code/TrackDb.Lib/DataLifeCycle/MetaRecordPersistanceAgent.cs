using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib.DataLifeCycle
{
    internal class MetaRecordPersistanceAgent : RecordPersistanceAgentBase
    {
        public MetaRecordPersistanceAgent(Database database)
            : base(database)
        {
        }

        public override void Run(DataManagementActivity forcedActivity, TransactionContext tx)
        {
            RunPersistence(forcedActivity, tx);
        }

        protected override int MaxInMemoryDataRecords =>
            Database.DatabasePolicy.InMemoryPolicy.MaxMetaDataRecords;

        protected override IEnumerable<Table> GetTables(
            DataManagementActivity forcedActivity,
            TransactionContext tx)
        {
            var tableMap = Database.GetDatabaseStateSnapshot().TableMap;
            var tableProperties = tableMap.Values
                .Where(tp => tp.IsMetaDataTable)
                .Where(tp => tp.IsPersisted);

            if (DoPersistAll(forcedActivity))
            {
                //  We limit to only 1st level metadata tables
                tableProperties = tableProperties
                    .Where(tp => !(((MetadataTableSchema)tp.Table.Schema).ParentSchema
                    is MetadataTableSchema));
            }

            return tableProperties
                .Select(tp => tp.Table);
        }

        protected override bool DoPersistAll(DataManagementActivity forcedActivity)
        {
            var doPersistEverything =
                (forcedActivity & DataManagementActivity.PersistAllMetaDataFirstLevel) != 0;

            return doPersistEverything;
        }

        protected override bool MergeTable(Table table, TransactionContext tx)
        {
            var loadedResult = tx.LoadCommittedBlocksInTransaction(table.Schema.TableName);
            var blockMergingLogic = new BlockMergingLogic(Database);
            var mergingResult = blockMergingLogic.MergeBlocks(
                table.Schema.TableName,
                null,
                Array.Empty<int>(),
                tx);

            return loadedResult || mergingResult;
        }
    }
}

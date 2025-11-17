using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib.Predicate;
using TrackDb.Lib.SystemData;

namespace TrackDb.Lib.DataLifeCycle
{
    internal abstract class HardDeleteAgentBase : BlockMergingAgentBase
    {
        #region Inner types
        protected record TableCandidate(string TableName, long DeletedRecordId);
        #endregion

        public HardDeleteAgentBase(
            Database database,
            TypedTable<TombstoneRecord> tombstoneTable,
            Lazy<DatabaseFileManager> storageManager)
            : base(database, tombstoneTable, storageManager)
        {
        }

        public override bool Run(DataManagementActivity forcedDataManagementActivity)
        {
            using (var tx = Database.CreateTransaction())
            {
                var doHardDeleteAll =
                    (forcedDataManagementActivity & DataManagementActivity.HardDeleteAll) != 0;
                var candidate = FindTransactionMergedCandidate(doHardDeleteAll, tx);

                if (candidate != null)
                {
                    HardDeleteCandidate(tx, candidate);

                    return false;
                }
                else
                {
                    return true;
                }
            }
        }

        protected abstract TableCandidate? FindCandidate(bool doHardDeleteAll, TransactionContext tx);

        private TableCandidate? FindTransactionMergedCandidate(
            bool doHardDeleteAll,
            TransactionContext tx)
        {
            TableCandidate? tableRecord = FindCandidate(doHardDeleteAll, tx);

            while (tableRecord != null)
            {
                if (tx.LoadCommittedBlocksInTransaction(tableRecord.TableName))
                {
                    var newTableRecord = FindCandidate(doHardDeleteAll, tx);

                    if (newTableRecord == tableRecord)
                    {
                        return tableRecord;
                    }
                    else
                    {
                        tableRecord = newTableRecord;
                        //  Re-loop if null, otherwise will return null
                    }
                }
                else
                {
                    return tableRecord;
                }
            }

            return null;
        }



        private void HardDeleteCandidate(TransactionContext tx, TableCandidate candidate)
        {
            //  The candidate can't be in-memory block since table was transaction merged
            var tableMap = Database.GetDatabaseStateSnapshot().TableMap;
            var table = Database.GetAnyTable(candidate.TableName);
            var metadataTable = Database.GetAnyTable(
                tableMap[candidate.TableName].MetaDataTableName!);
            //  Find block owning candidate tombstoned record
            var candidateBlockId = FindCandidateBlock(table, candidate.DeletedRecordId, tx);

            if (candidateBlockId != null)
            {
                //  Find meta block owning that block
                var candidateMetaBlockId =
                    FindCandidateMetaBlock(metadataTable, candidateBlockId.Value, tx);

                //  Do a block merge on the meta block
                MergeBlocksUnder(metadataTable.Schema.TableName, candidateMetaBlockId, tx);
            }
            else
            {   //  Record doesn't exist:  likely a racing condition between 2 transactions
                //  (should be rare)
                throw new NotImplementedException();
            }
        }

        private int? FindCandidateBlock(
            Table table,
            long candidateDeletedRecordId,
            TransactionContext tx)
        {
            var predicate = new BinaryOperatorPredicate(
                table.Schema.RecordIdColumnIndex,
                candidateDeletedRecordId,
                BinaryOperator.Equal);
            var blockId = table.Query(tx)
                .WithIgnoreDeleted()
                .WithPredicate(predicate)
                .WithProjection([table.Schema.ParentBlockIdColumnIndex])
                .Select(r => (int)r.Span[0]!)
                .FirstOrDefault();

            return blockId;
        }

        private int FindCandidateMetaBlock(Table metadataTable, int blockId, TransactionContext tx)
        {
            var metadataSchema = (MetadataTableSchema)metadataTable.Schema;
            var predicate = new BinaryOperatorPredicate(
                metadataSchema.BlockIdColumnIndex,
                blockId,
                BinaryOperator.Equal);
            var metaBlockId = metadataTable.Query(tx)
                .WithPredicate(predicate)
                .WithProjection([metadataSchema.ParentBlockIdColumnIndex])
                .Select(r => (int)r.Span[0]!)
                .First();

            return metaBlockId;
        }
    }
}
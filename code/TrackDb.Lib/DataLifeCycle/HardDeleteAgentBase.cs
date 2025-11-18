using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Reflection.Emit;
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
                    MergeCandidate(candidate, tx);
                }

                tx.Complete();

                return candidate == null;
            }
        }

        private void MergeCandidate(TableCandidate candidate, TransactionContext tx)
        {
            var tableMap = Database.GetDatabaseStateSnapshot().TableMap;
            var properties = tableMap[candidate.TableName];

            if (properties.IsMetaDataTable)
            {
                throw new InvalidOperationException(
                    $"Table '{candidate.TableName}' has tombstoned records ; " +
                    $"this should be impossible for a metadata table");
            }
            if (properties.MetaDataTableName == null)
            {
                throw new InvalidOperationException(
                    $"Table '{candidate.TableName}' has tombstoned records but" +
                    $"no metadata table");
            }

            //  Find block owning candidate tombstoned record
            var candidateBlockId = FindCandidateBlock(
                candidate.TableName,
                candidate.DeletedRecordId,
                tx);

            if (candidateBlockId != null)
            {
                //  Do a block merge
                MergeBlock(properties.MetaDataTableName, candidateBlockId.Value, tx);
            }
            else
            {   //  Record doesn't exist anymore:  likely a racing condition between 2 transactions
                //  (should be rare)
                Database.TombstoneTable.Query(tx)
                    .Where(pf => pf.Equal(t => t.TableName, candidate.TableName))
                    .Where(pf => pf.Equal(t => t.DeletedRecordId, candidate.DeletedRecordId))
                    .Delete();
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

        private int? FindCandidateBlock(
            string tableName,
            long candidateDeletedRecordId,
            TransactionContext tx)
        {
            var table = Database.GetAnyTable(tableName);
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

            return blockId > 0 ? blockId : null;
        }
    }
}
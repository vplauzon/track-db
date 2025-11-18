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
                var isCompleted = false;

                do
                {
                    var candidate = FindTransactionMergedCandidate(doHardDeleteAll, tx);

                    if (candidate != null)
                    {
                        MergeCandidate(candidate, tx);
                    }
                    else
                    {
                        isCompleted = true;
                    }
                }
                while (!isCompleted);

                tx.Complete();

                return true;
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

            MergeDataBlocks(
                candidate.TableName,
                candidate.DeletedRecordId,
                tx);
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
    }
}
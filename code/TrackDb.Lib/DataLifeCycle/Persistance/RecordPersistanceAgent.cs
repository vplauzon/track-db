using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using TrackDb.Lib.InMemory.Block;

namespace TrackDb.Lib.DataLifeCycle.Persistance
{
    internal class RecordPersistanceAgent : DataLifeCycleAgentBase
    {
        private readonly IPersistanceCandidateProvider _persistanceCandidateProvider;

        public RecordPersistanceAgent(
            Database database,
            IPersistanceCandidateProvider persistanceCandidateProvider)
            : base(database)
        {
            _persistanceCandidateProvider = persistanceCandidateProvider;
        }

        public override void Run(DataManagementActivity activity, TransactionContext tx)
        {
            foreach (var candidate in _persistanceCandidateProvider.FindCandidates(activity, tx))
            {
                PersistTable(candidate, tx);
            }
        }

        private void PersistTable(PersistanceCandidate candidate, TransactionContext tx)
        {
            tx.LoadCommittedBlocksInTransaction(candidate.Table.Schema.TableName);

            //  We persist as much blocks from the table as possible
            var committedDataBlock = tx.TransactionState.UncommittedTransactionLog
                .TransactionTableLogMap[candidate.Table.Schema.TableName]
                .CommittedDataBlock;

            if (committedDataBlock == null)
            {
                throw new InvalidOperationException("CommittedDataBlock shouldn't be null");
            }

            committedDataBlock.OrderByRecordId();

            IBlock tableBlock = committedDataBlock;
            var metadataTable = Database.GetMetaDataTable(tableBlock.TableSchema.TableName);
            var metaSchema = (MetadataTableSchema)metadataTable.Schema;
            var buffer = new byte[Database.DatabasePolicy.StoragePolicy.BlockSize];
            var segments = committedDataBlock.SegmentRecords(buffer.Length);
            var blockIds = Database.UseAvailableBlockIds(segments.Count, tx);
            var skipRows = 0;

            for (int i = 0; i != segments.Count; ++i)
            {
                var blockStats =
                    committedDataBlock.Serialize(buffer, skipRows, segments[i].ItemCount);

                if (blockStats.Size != segments[i].Size)
                {
                    throw new InvalidOperationException(
                        $"Block size ({blockStats.Size}) is different than planned" +
                        $" ({segments[i].Size})");
                }

                Database.PersistBlock(blockIds[i], buffer.AsSpan().Slice(0, blockStats.Size), tx);
                metadataTable.AppendRecord(
                    metaSchema.CreateMetadataRecord(blockIds[i], blockStats).Span,
                    tx);
                skipRows += blockStats.ItemCount;
            }
            committedDataBlock.Clear();
        }
    }
}
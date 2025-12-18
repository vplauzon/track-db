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
            var tableBlockBuilder = tx.TransactionState.UncommittedTransactionLog
                .TransactionTableLogMap[candidate.Table.Schema.TableName]
                .CommittedDataBlock;

            if (tableBlockBuilder == null)
            {
                throw new InvalidOperationException("CommittedDataBlock shouldn't be null");
            }

            tableBlockBuilder.OrderByRecordId();

            IBlock tableBlock = tableBlockBuilder;
            var metadataTable = Database.GetMetaDataTable(tableBlock.TableSchema.TableName);
            var metaSchema = (MetadataTableSchema)metadataTable.Schema;
            var buffer = new byte[Database.DatabasePolicy.StoragePolicy.BlockSize];
            var segments = tableBlockBuilder.SegmentRecords(buffer.Length);
            var skipRows = 0;

            foreach (var segment in segments)
            {
                var blockStats =
                    tableBlockBuilder.Serialize(buffer, skipRows, segment.ItemCount);

                if (blockStats.Size != segment.Size)
                {
                    throw new InvalidOperationException(
                        $"Block size ({blockStats.Size}) is bigger than planned" +
                        $"maximum ({segment.Size})");
                }
                var blockId = Database.GetAvailableBlockId(tx);

                Database.PersistBlock(blockId, buffer.AsSpan().Slice(0, blockStats.Size), tx);
                metadataTable.AppendRecord(
                    metaSchema.CreateMetadataRecord(blockId, blockStats).Span,
                    tx);
                skipRows += blockStats.ItemCount;
            }
            tableBlockBuilder.Clear();
        }
    }
}
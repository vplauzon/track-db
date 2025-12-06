using System;
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

        public override void Run(
            DataManagementActivity activity,
            TransactionContext tx)
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

            IBlock tableBlock = tableBlockBuilder;
            var metadataTable = Database.GetMetaDataTable(tableBlock.TableSchema.TableName);
            var metaSchema = (MetadataTableSchema)metadataTable.Schema;
            var isFirstBlockToPersist = true;
            var buffer = new byte[Database.DatabasePolicy.StoragePolicy.BlockSize];
            var skipRows = 0;

            tableBlockBuilder.OrderByRecordId();
            while (tableBlock.RecordCount - skipRows > 0)
            {
                var blockStats = tableBlockBuilder.TruncateSerialize(buffer, skipRows);

                if (blockStats.ItemCount == 0)
                {
                    throw new InvalidDataException(
                        $"A single record is too large to persist on table " +
                        $"'{tableBlock.TableSchema.TableName}' with " +
                        $"{tableBlock.TableSchema.Columns.Count} columns");
                }
                if (blockStats.Size > buffer.Length)
                {
                    throw new IndexOutOfRangeException(
                        $"Buffer overrun:  {blockStats.Size}>{buffer.Length}");
                }

                //  We stop before persisting the last (typically incomplete) block
                if (isFirstBlockToPersist
                    || candidate.DoPersistAll
                    || tableBlock.RecordCount - skipRows > blockStats.ItemCount)
                {
                    if (blockStats.Size > buffer.Length)
                    {
                        throw new InvalidOperationException(
                            $"Block size ({blockStats.Size}) is bigger than planned" +
                            $"maximum ({buffer.Length})");
                    }

                    var blockId = Database.PersistBlock(buffer.AsSpan().Slice(0, blockStats.Size), tx);

                    metadataTable.AppendRecord(
                        metaSchema.CreateMetadataRecord(blockId, blockStats).Span,
                        tx);
                    isFirstBlockToPersist = false;
                    skipRows += blockStats.ItemCount;
                }
                else
                {   //  We're done
                    break;
                }
            }
            tableBlockBuilder.DeleteRecordsByRecordIndex(Enumerable.Range(0, skipRows));
        }
    }
}
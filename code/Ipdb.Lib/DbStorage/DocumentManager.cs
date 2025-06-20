using Ipdb.Lib.Cache;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;

namespace Ipdb.Lib.DbStorage
{
    /// <summary>
    /// Responsible for reading and writing document blocks.
    /// A document block has the following layout:
    /// DocumentCount (short)
    /// Header:  for each document, Revision ID (long) & Length(short)
    /// Payloads:  for each document, Payload bytes (of length advertised in the header)
    /// </summary>
    internal class DocumentManager : DataManagerBase
    {
        #region Inner types
        [StructLayout(LayoutKind.Sequential, Pack = 1)]
        private readonly record struct Header(long RevisionId, short Length);
        #endregion

        public DocumentManager(StorageManager storageManager)
            : base(storageManager)
        {
        }

        //public long AppendDocument(object document)
        //{
        //    if (document == null)
        //    {
        //        throw new ArgumentNullException(nameof(document));
        //    }

        //    var revisionId = Interlocked.Increment(ref _revisionId);
        //    var serializedDocument = Serialize(document);
        //    var blockId = StorageManager.ReserveBlock();

        //    if (serializedDocument.Length * sizeof(byte)
        //        > StorageManager.BlockSize - sizeof(short))
        //    {
        //        throw new ArgumentOutOfRangeException(
        //            nameof(document),
        //            $"Document size:  {serializedDocument.Length}");
        //    }
        //    using (var accessor = StorageManager.CreateViewAccessor(blockId, false))
        //    {
        //        var startOffset = 0;
        //        var offset = startOffset;

        //        accessor.Write(offset, revisionId);
        //        offset += sizeof(long);
        //        accessor.Write(offset, (short)(serializedDocument.Length * sizeof(byte)));
        //        offset += sizeof(short);
        //        accessor.WriteArray(offset, serializedDocument, 0, serializedDocument.Length);
        //        offset += serializedDocument.Length * sizeof(byte);

        //        return revisionId;
        //    }
        //}

        #region Persist documents
        public DatabaseCache? PersistDocuments(DatabaseCache cache, bool doPersistEverything)
        {
            return !cache.TransactionLogs.Any() || !cache.TransactionLogs.First().NewDocuments.Any()
                ? null
                : PersistsNewDocuments(cache, doPersistEverything)
                ?? DeleteDocuments(cache, doPersistEverything);
        }

        private DatabaseCache? PersistsNewDocuments(DatabaseCache cache, bool doPersistEverything)
        {
            var remainingSize = StorageManager.BlockSize - sizeof(short);
            var transactionLog = cache.TransactionLogs.First();
            var newDocuments = transactionLog.NewDocuments
                .OrderBy(o => o.Key)
                .ToImmutableArray();
            var documentCount = 0;

            while (documentCount < newDocuments.Length)
            {
                var document = newDocuments[documentCount];
                var newRemainingSize = remainingSize
                    - Marshal.SizeOf<Header>()
                    - sizeof(byte) * document.Value.Length;

                if (newRemainingSize > 0)
                {
                    remainingSize = newRemainingSize;
                    ++documentCount;
                }
                else if (documentCount == 0)
                {
                    throw new InvalidOperationException(
                        $"Document size exceed capacity:  " +
                        $"{sizeof(byte) * document.Value.Length}");
                }
                else
                {
                    break;
                }
            }
            if (documentCount > 0
                //  We want to have remaining documents:  proving we max out block
                && (documentCount < newDocuments.Length || doPersistEverything))
            {
                var newNewDocuments = newDocuments
                    .Skip(documentCount)
                    .ToImmutableDictionary(p => p.Key, p => p.Value);
                var newBlock = PersistNewBlock(newDocuments.Take(documentCount));
                var newCache = new DatabaseCache(
                    cache.TransactionLogs.Prepend(
                        transactionLog with { NewDocuments = newNewDocuments })
                    .ToImmutableArray(),
                    cache.DocumentBlocks.AddBlock(newBlock),
                    cache.IndexBlocks);

                return newCache;
            }
            else
            {
                return null;
            }
        }

        private DocumentBlock PersistNewBlock(
            IEnumerable<KeyValuePair<long, byte[]>> documents)
        {
            var writer = StorageManager.GetBlockWriter();
            var header = documents
                .Select(d => new Header(d.Key, (short)d.Value.Length));
            var documentPayload = documents
                .SelectMany(d => d.Value);

            writer.Write((short)documents.Count());
            writer.WriteArray(header.ToArray());
            writer.WriteArray(documentPayload.ToArray());

            var block = writer.ToBlock();
            var documentBlock = new DocumentBlock(
                block,
                documents.Min(p => p.Key),
                documents.Max(p => p.Key));

            return documentBlock;
        }

        private DatabaseCache? DeleteDocuments(DatabaseCache cache, bool doPersistEverything)
        {
            return null;
        }
        #endregion
    }
}
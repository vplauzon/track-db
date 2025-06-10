using Ipdb.Lib.Document;
using System;
using System.Buffers;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Linq.Expressions;
using System.Text.Json;

namespace Ipdb.Lib
{
    public class Table<T>
    {
        #region Inner Types
        private record DocumentIndexInfo(
            long Offset,
            short PrimaryIndexHash,
            IImmutableList<short> SecondaryIndexHashes);
        #endregion

        private readonly int _tableIndex;
        private readonly TableSchema<T> _schema;
        private readonly StorageManager _storageManager;

        #region Constructors
        internal Table(
            int tableIndex,
            TableSchema<T> schema,
            StorageManager storageManager)
        {
            _tableIndex = tableIndex;
            _schema = schema;
            _storageManager = storageManager;
        }
        #endregion

        public IEnumerable<T> Query(Expression<Func<T, bool>> predicate)
        {
            throw new NotImplementedException();
        }

        public void AppendDocuments(params IEnumerable<T> documents)
        {
            var documentIndexInfos = new List<DocumentIndexInfo> ();

            foreach (var document in documents)
            {
                if (document == null)
                {
                    throw new ArgumentNullException(nameof(documents));
                }

                var primaryIndex = _schema.PrimaryIndex.ObjectExtractor(document);
                var secondaryIndexes = _schema.SecondaryIndexes
                    .Select(i => i.ObjectExtractor(document))
                    .ToImmutableArray();
                var metaData = new DocumentMetaData(_tableIndex, primaryIndex, secondaryIndexes);
                var serializedMetaData = Serialize(metaData);
                var serializedDocument = Serialize(document);
                var offset = _storageManager.DocumentManager.AppendDocument(
                    serializedMetaData,
                    serializedDocument);
                
                documentIndexInfos.Add(new DocumentIndexInfo(offset,));
            }
        }

        public long DeleteDocuments(Expression<Func<T, bool>> predicate)
        {
            throw new NotImplementedException();
        }

        #region Serialization
        private byte[] Serialize(object document)
        {
            var bufferWriter = new ArrayBufferWriter<byte>();

            using (var writer = new Utf8JsonWriter(bufferWriter))
            {
                JsonSerializer.Serialize(writer, document);
            }

            return bufferWriter.WrittenMemory.ToArray();
        }
        #endregion
    }
}

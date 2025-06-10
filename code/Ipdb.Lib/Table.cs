using Ipdb.Lib.Document;
using System;
using System.Buffers;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq.Expressions;
using System.Text.Json;

namespace Ipdb.Lib
{
    public class Table<T>
    {
        private readonly TableSchema<T> _schema;
        private readonly StorageManager _storageManager;

        #region Constructors
        internal Table(TableSchema<T> schema, StorageManager storageManager)
        {
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
            foreach (var document in documents)
            {
                if (document == null)
                {
                    throw new ArgumentNullException(nameof(documents));
                }

                var primaryIndex = _schema.PrimaryIndex.ObjectExtractor(document);
                var documentIndexes = new DocumentIndexes(primaryIndex);
                var serializedDocumentIndexes = Serialize(documentIndexes);
                var serializedDocument = Serialize(document);

                _storageManager.DocumentManager.AppendDocument(
                    serializedDocumentIndexes,
                    serializedDocument);
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

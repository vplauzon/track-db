using System;
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
                var serializedDocument = Serialize(document);
                //_storageManager.DocumentManager.AppendDocuments();
            }
        }

        public long DeleteDocuments(Expression<Func<T, bool>> predicate)
        {
            throw new NotImplementedException();
        }

        #region Serialization
        private byte[] Serialize(T document)
        {
            //Utf8JsonWriter?
            using (var stream = new MemoryStream())
            {
                JsonSerializer.Serialize(stream, document);

                return stream.ToArray();
            }
        }
        #endregion
    }
}

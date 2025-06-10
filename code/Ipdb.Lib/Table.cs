using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;

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
            //_storageManager.DocumentManager.AppendDocuments();
        }

        public long DeleteDocuments(Expression<Func<T, bool>> predicate)
        {
            throw new NotImplementedException();
        }
    }
}

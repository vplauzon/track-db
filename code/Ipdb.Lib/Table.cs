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
        private readonly DataManager _storageManager;

        #region Constructors
        internal Table(
            int tableIndex,
            TableSchema<T> schema,
            DataManager storageManager)
        {
            _tableIndex = tableIndex;
            _schema = schema;
            _storageManager = storageManager;
        }
        #endregion

        public IEnumerable<T> Query(Expression<Func<T, bool>> predicate)
        {
            throw new NotSupportedException(
                "Only predicates to primary or secondary indexes are supported");
        }

        public void AppendDocuments(params IEnumerable<T> documents)
        {
            var documentIndexInfos = new List<DocumentIndexInfo>();

            foreach (var document in documents)
            {
                if (document == null)
                {
                    throw new ArgumentNullException(nameof(documents));
                }

                //  Persist the document itself
                var documentPosition = _storageManager.DocumentManager.AppendDocument(
                    _tableIndex,
                    document);
                var primaryIndex = _schema.PrimaryIndex.ObjectExtractor(document);
                var secondaryIndexes = _schema.SecondaryIndexes
                    .Select(i => i.ObjectExtractor(document))
                    .ToImmutableArray();
                var allIndexes = new DocumentAllIndexes(
                    primaryIndex.FullValue,
                    secondaryIndexes
                    .Select(v => v.FullValue)
                    .ToImmutableArray(),
                    documentPosition);

                //  Persist all indexes
                _storageManager.PrimaryIndexManager.AppendIndexes(_tableIndex, allIndexes);
            }
        }

        public long DeleteDocuments(Expression<Func<T, bool>> predicate)
        {
            throw new NotImplementedException();
        }
    }
}

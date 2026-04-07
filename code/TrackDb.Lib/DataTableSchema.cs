using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace TrackDb.Lib
{
    /// <summary>
    /// Represents the schema of a table holding actual data (as opposed to metadata).
    /// </summary>
    public class DataTableSchema : TableSchema
    {
        internal const string RECORD_ID = "$recordId";

        public DataTableSchema(
            string tableName,
            IEnumerable<ColumnSchema> columns,
            IEnumerable<int> primaryKeyColumnIndexes,
            IEnumerable<int> partitionKeyColumnIndexes,
            IEnumerable<TableTriggerAction> triggerActions)
            : base(
                  tableName,
                  columns.Select(c => ColumnSchemaProperties.CreateGenerationZero(c)),
                  [ColumnSchemaProperties.CreateGenerationZero(new(RECORD_ID, typeof(long), true))],
                  primaryKeyColumnIndexes.ToImmutableArray(),
                  partitionKeyColumnIndexes.ToImmutableArray(),
                  triggerActions)
        {
            RecordIdColumnIndex = ColumnProperties.Count - 1;
        }

        internal override bool IsMetadata => false;

        #region Extra columns
        public int RecordIdColumnIndex {get;}
        #endregion
    }
}
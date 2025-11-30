using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;

namespace TrackDb.Lib.DataLifeCycle
{
    internal class TombstoneBlockFixLogic : LogicBase
    {
        public TombstoneBlockFixLogic(Database database)
            : base(database)
        {
        }

        /// <summary>Fix <c>null</c> block ids in tombstone records.</summary>
        /// <param name="tableName"></param>
        /// <param name="tx"></param>
        public void FixNullBlockIds(string tableName, TransactionContext tx)
        {
            tx.LoadCommittedBlocksInTransaction(tableName);

            var orphansDeletedRecordId = Database.TombstoneTable.Query(tx)
                .Where(pf => pf.Equal(t => t.TableName, tableName))
                .Where(pf => pf.Equal(t => t.BlockId, null))
                .Select(t => t.DeletedRecordId)
                .ToImmutableArray();

            if (orphansDeletedRecordId.Any())
            {
                throw new NotImplementedException();
            }
        }
    }
}
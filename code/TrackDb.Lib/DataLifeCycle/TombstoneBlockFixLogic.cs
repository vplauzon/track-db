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
        /// <returns><c>true</c> iif <c>null</c> block IDs were found and fixed.</returns>
        public bool FixNullBlockIds(string tableName, TransactionContext tx)
        {
            var orphansDeletedRecordId = Database.TombstoneTable.Query(tx)
                .Where(pf => pf.Equal(t => t.TableName, tableName))
                .Where(pf => pf.Equal(t => t.BlockId, null))
                .Select(t => t.DeletedRecordId)
                .ToImmutableArray();

            if (orphansDeletedRecordId.Length > 0)
            {
                throw new NotImplementedException();
            }
            else
            {
                return false;
            }
        }
    }
}
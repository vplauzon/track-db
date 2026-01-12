using System;
using System.Collections.Generic;
using System.Text;

namespace TrackDb.Lib.DataLifeCycle
{
    internal class BlockMergingLogic2 : LogicBase
    {
        public BlockMergingLogic2(Database database)
            : base(database)
        {
        }

        /// <summary>
        /// Merge together blocks belonging to <paramref name="metaBlockId"/>.
        /// If <paramref name="tableName"/> is a data table, i.e. is at the lowest level,
        /// it will compact tombstoned records.
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="metaBlockId">If <c>null</c>, use in-memory meta block.</param>
        /// <param name="tx"></param>
        /// <returns></returns>
        public int CompactMerge(
            string tableName,
            int? metaBlockId,
            TransactionContext tx)
        {
            throw new NotImplementedException();
        }
    }
}
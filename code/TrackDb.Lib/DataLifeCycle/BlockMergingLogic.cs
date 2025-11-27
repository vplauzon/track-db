using System;
using System.Collections.Generic;
using System.Text;

namespace TrackDb.Lib.DataLifeCycle
{
    internal class BlockMergingLogic : LogicBase
    {
        public BlockMergingLogic(Database database)
            : base(database)
        {
        }

        /// <summary>
        /// Compact block <paramref name="blockId"/> in table <paramref name="dataTableName"/>.
        /// This block is going to be compacted and then merged with adjacent blocks.
        /// If any block in <paramref name="otherBlockIdsToCompact"/> is encountered during
        /// those merge operations, they are going to be compacted.  If not, they won't.
        /// </summary>
        /// <param name="dataTableName"></param>
        /// <param name="blockId"></param>
        /// <param name="otherBlockIdsToCompact"></param>
        /// <param name="tx"></param>
        /// <returns><c>false</c> iif <paramref name="blockId"/> doesn't exist</returns>
        public bool Compact(
            string dataTableName,
            int blockId,
            IEnumerable<int> otherBlockIdsToCompact,
            TransactionContext tx)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Merge meta block <paramref name="metaBlockId"/> in meta table
        /// <paramref name="metaTableName"/> and compact blocks in
        /// <paramref name="blockIdsToCompact"/> if they are encountered.
        /// </summary>
        /// <param name="metaTableName"></param>
        /// <param name="metaBlockId"></param>
        /// <param name="blockIdsToCompact"></param>
        /// <param name="tx"></param>
        public void Merge(
            string metaTableName,
            int metaBlockId,
            IEnumerable<int> blockIdsToCompact,
            TransactionContext tx)
        {
            throw new NotImplementedException();
        }
    }
}
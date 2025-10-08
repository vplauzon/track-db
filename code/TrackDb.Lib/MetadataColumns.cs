using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace TrackDb.Lib
{
    /// <summary>
    /// Database:  a collection of tables that can share transactions
    /// and are persisted in the same file.
    /// </summary>
    internal static class MetadataColumns
    {
        public const string ITEM_COUNT = "$itemCount";

        public const string BLOCK_ID = "$blockId";
    }
}
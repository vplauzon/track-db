using TrackDb.Lib.InMemory;
using TrackDb.Lib.InMemory.Block;
using TrackDb.Lib.DbStorage;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using TrackDb.Lib.Policies;
using TrackDb.Lib.DataLifeCycle;

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
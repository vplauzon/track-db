using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
using System.Linq;
using System.Text;

namespace TrackDb.Lib.DataLifeCycle
{
    internal class TombstoneBlockLogic : LogicBase
    {
        #region Inner Types
        #endregion

        public TombstoneBlockLogic(Database database)
            : base(database)
        {
        }

        //public bool CompactMerge(string tableName, int? metaBlockId)
        //{
        //}
    }
}
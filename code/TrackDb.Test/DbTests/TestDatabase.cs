using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib;
using TrackDb.Lib.Settings;

namespace TrackDb.Test.DbTests
{
    internal class TestDatabase : Database
    {
        #region Entity types
        public record IntOnly(int Integer);
        #endregion

        private const string INT_ONLY_TABLE = "IntOnly";

        public TestDatabase()
            : base(
                 new DatabaseSettings(),
                 TypedTableSchema<IntOnly>.FromConstructor(INT_ONLY_TABLE))
        {
        }

        public TypedTable<IntOnly> IntOnlyTable => GetTypedTable<IntOnly>(INT_ONLY_TABLE);
    }
}
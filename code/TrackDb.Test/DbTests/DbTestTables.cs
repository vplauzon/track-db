using TrackDb.Lib;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Tests.DbTests
{
    internal static class DbTestTables
    {
        #region Entity types
        public record IntOnly(int Integer);
        #endregion

        private const string TABLE_NAME = "MyTable";

        public static DbTestTable<IntOnly> CreateIntOnly()
        {
            return new DbTestTable<IntOnly>(TypedTableSchema<IntOnly>.FromConstructor(TABLE_NAME));
        }
    }
}
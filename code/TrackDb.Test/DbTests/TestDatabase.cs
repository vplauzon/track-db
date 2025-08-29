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
        public record Primitives(int Integer, int? NullableInteger = null);
        #endregion

        private const string PRIMITIVES_TABLE = "Primitives";

        public TestDatabase()
            : base(
                 new DatabaseSettings(),
                 TypedTableSchema<Primitives>.FromConstructor(PRIMITIVES_TABLE))
        {
        }

        public TypedTable<Primitives> PrimitiveTable => GetTypedTable<Primitives>(PRIMITIVES_TABLE);
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib;
using TrackDb.Lib.Policies;

namespace TrackDb.Test.DbTests
{
    internal class TestDatabase : Database
    {
        #region Entity types
        public record Primitives(int Integer, int? NullableInteger = null);
        
        public record MultiIntegers(int Integer1, int Integer2, int Integer3, int Integer4);
        
        public record FullName(string FirstName, string LastName);
        
        public record VersionedName(FullName FullName, int Version);

        public record CompoundKeys(VersionedName VersionedId, short Value);
        #endregion

        private const string PRIMITIVES_TABLE = "Primitives";
        private const string MULTI_INTEGERS_TABLE = "MultiIntegers";
        private const string COMPOUND_KEYS_TABLE = "CompoundKeys";

        public TestDatabase()
            : base(
                 new DatabasePolicies(),
                 TypedTableSchema<Primitives>.FromConstructor(PRIMITIVES_TABLE),
                 TypedTableSchema<MultiIntegers>.FromConstructor(MULTI_INTEGERS_TABLE),
                 TypedTableSchema<CompoundKeys>.FromConstructor(COMPOUND_KEYS_TABLE))
        {
        }

        public TypedTable<Primitives> PrimitiveTable
            => GetTypedTable<Primitives>(PRIMITIVES_TABLE);

        public TypedTable<MultiIntegers> MultiIntegerTable
            => GetTypedTable<MultiIntegers>(MULTI_INTEGERS_TABLE);

        public TypedTable<CompoundKeys> CompoundKeyTable
            => GetTypedTable<CompoundKeys>(COMPOUND_KEYS_TABLE);
    }
}
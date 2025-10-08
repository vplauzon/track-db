using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib;
using TrackDb.Lib.Policies;

namespace TrackDb.Test.DbTests
{
    internal class TestDatabase : IAsyncDisposable
    {
        #region Entity types
        public record Primitives(int Integer, int? NullableInteger = null);

        public record MultiIntegers(int Integer1, int Integer2, int Integer3, int Integer4);

        public record FullName(string FirstName, string LastName);

        public record VersionedName(int Version, FullName FullName);

        public record CompoundKeys(VersionedName VersionedName, short Value);

        public record OtherTypes(Uri Uri);
        #endregion

        private const string PRIMITIVES_TABLE = "Primitives";
        private const string MULTI_INTEGERS_TABLE = "MultiIntegers";
        private const string COMPOUND_KEYS_TABLE = "CompoundKeys";
        private const string OTHER_TYPES_TABLE = "OtherTypes";

        #region Constructor
        public static async Task<TestDatabase> CreateAsync()
        {
            var logFolderText = TestConfiguration.Instance.GetConfiguration("logFolderUri");
            var logFolderUri = logFolderText == null ? null : new Uri(logFolderText);
            var db = await Database.CreateAsync(
                DatabasePolicy.Create(LogPolicy: LogPolicy.Create(logFolderUri)),
                TypedTableSchema<Primitives>.FromConstructor(PRIMITIVES_TABLE),
                TypedTableSchema<MultiIntegers>.FromConstructor(MULTI_INTEGERS_TABLE)
                .AddPrimaryKeyProperty(m => m.Integer1),
                TypedTableSchema<CompoundKeys>.FromConstructor(COMPOUND_KEYS_TABLE)
                .AddPrimaryKeyProperty(m => m.VersionedName.FullName),
                TypedTableSchema<OtherTypes>.FromConstructor(OTHER_TYPES_TABLE));

            return new TestDatabase(db);
        }

        private TestDatabase(Database database)
        {
            Database = database;
        }
        #endregion

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            await ((IAsyncDisposable)Database).DisposeAsync();
        }

        public Database Database { get; }

        public TypedTable<Primitives> PrimitiveTable
            => Database.GetTypedTable<Primitives>(PRIMITIVES_TABLE);

        public TypedTable<MultiIntegers> MultiIntegerTable
            => Database.GetTypedTable<MultiIntegers>(MULTI_INTEGERS_TABLE);

        public TypedTable<CompoundKeys> CompoundKeyTable
            => Database.GetTypedTable<CompoundKeys>(COMPOUND_KEYS_TABLE);

        public TypedTable<OtherTypes> OtherTypesTable
            => Database.GetTypedTable<OtherTypes>(OTHER_TYPES_TABLE);
    }
}
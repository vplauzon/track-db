using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace Ipdb.Lib
{
    public class Engine : IAsyncDisposable
    {
        private readonly string _localRootDirectory;
        private readonly IDictionary<string, Database> _databaseMap =
            new Dictionary<string, Database>();

        #region Constructors
        public Engine(string? localRootDirectory)
        {
            _localRootDirectory = localRootDirectory
                ?? Path.Combine(Path.GetTempPath(), "ipdb", Guid.NewGuid().ToString());
        }
        #endregion

        public async Task<Database> LoadDatabaseAsync(string databaseName, DatabaseSchema schema)
        {
            await Task.CompletedTask;

            var dbFolder = Path.Combine(_localRootDirectory, databaseName);
            var database = new Database(dbFolder, schema);

            _databaseMap.Add(databaseName, database);

            return database;
        }

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            foreach (IAsyncDisposable database in _databaseMap.Values)
            {
                await database.DisposeAsync();
            }
        }
    }
}
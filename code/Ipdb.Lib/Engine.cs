using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace Ipdb.Lib
{
    public class Engine : IDisposable
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
            var database = new Database(Path.Combine(dbFolder, databaseName), schema);

            _databaseMap.Add(databaseName, database);

            return database;
        }

        void IDisposable.Dispose()
        {
            foreach (IDisposable database in _databaseMap.Values)
            {
                database.Dispose();
            }
        }
    }
}
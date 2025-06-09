using System;
using System.IO;
using System.Threading.Tasks;

namespace Ipdb.Lib
{
    public class Engine
    {
        private readonly string _localRootDirectory;

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

            return database;
        }
    }
}
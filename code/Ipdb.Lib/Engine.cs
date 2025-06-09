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
            var dbFolder = Path.Combine(_localRootDirectory, databaseName);
            var database = new Database(dbFolder);

            await Task.CompletedTask;
            throw new NotImplementedException();
        }
    }
}
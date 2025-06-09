using System;
using System.Threading.Tasks;

namespace Ipdb.Lib
{
    public class Engine
    {
        private readonly string? _localRootDirectory;

        #region Constructors
        public Engine(string? localRootDirectory)
        {
            _localRootDirectory = localRootDirectory;
        }
        #endregion

        public Task<Database> LoadDatabaseAsync(string databaseName, DatabaseSchema schema)
        {
            throw new NotImplementedException();
        }
    }
}
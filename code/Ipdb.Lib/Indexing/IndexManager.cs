using System;

namespace Ipdb.Lib.Indexing
{
    public class IndexManager : IDisposable
    {
        private readonly string _databaseRootDirectory;

        public IndexManager(string databaseRootDirectory)
        {
            _databaseRootDirectory = databaseRootDirectory;
        }

        void IDisposable.Dispose()
        {
        }
    }
}
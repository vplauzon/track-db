using System;

namespace Ipdb.Lib.Indexing
{
    internal class IndexManager : IDisposable
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
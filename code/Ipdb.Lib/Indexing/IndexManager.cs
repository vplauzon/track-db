namespace Ipdb.Lib.Indexing
{
    public class IndexManager
    {
        private readonly string _databaseRootDirectory;

        public IndexManager(string databaseRootDirectory)
        {
            _databaseRootDirectory = databaseRootDirectory;
        }
    }
}
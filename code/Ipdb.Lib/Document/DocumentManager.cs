namespace Ipdb.Lib.Document
{
    public class DocumentManager
    {
        private readonly string _databaseRootDirectory;

        public DocumentManager(string databaseRootDirectory)
        {
            _databaseRootDirectory = databaseRootDirectory;
        }
    }
}
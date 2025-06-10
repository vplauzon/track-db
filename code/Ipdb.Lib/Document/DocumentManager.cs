using System.IO;

namespace Ipdb.Lib.Document
{
    public class DocumentManager
    {
        public DocumentManager(string databaseRootDirectory)
        {
            var filePath = Path.Combine(databaseRootDirectory, "documents.json");
        }
    }
}
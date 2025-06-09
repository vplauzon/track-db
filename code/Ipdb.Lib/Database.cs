using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ipdb.Lib
{
    public class Database
    {
        private readonly string _databaseRootDirectory;

        public Database(string dbFolder, DatabaseSchema schema)
        {
            _databaseRootDirectory = dbFolder;

            if (Directory.Exists(dbFolder))
            {
                throw new ArgumentException(
                    $"Database directory '{dbFolder}' already exists",
                    nameof(dbFolder));
            }
            else
            {
                Directory.CreateDirectory(dbFolder);
            }
        }

        public Table<T> GetTable<T>(string tableName)
        {
            throw new NotImplementedException();
        }
    }
}
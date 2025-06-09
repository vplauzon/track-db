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

        #region Constructor
        public Database(string dbFolder, DatabaseSchema schema)
        {
            _databaseRootDirectory = dbFolder;
            EnsureDirectory(dbFolder);
            foreach (var tableName in schema.TableMap.Keys)
            {
                var schemaObject = schema.TableMap[tableName];
            }
        }

        private static void EnsureDirectory(string dbFolder)
        {
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
        #endregion

        public Table<T> GetTable<T>(string tableName)
        {
            throw new NotImplementedException();
        }
    }
}
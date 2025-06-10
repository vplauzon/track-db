using Ipdb.Lib.Document;
using Ipdb.Lib.Indexing;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ipdb.Lib
{
    internal class StorageManager : IDisposable
    {
        #region Constructors
        public StorageManager(string databaseRootDirectory)
        {
            EnsureDirectory(databaseRootDirectory);
            DocumentManager = new(databaseRootDirectory);
            IndexManager = new(databaseRootDirectory);
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

        public DocumentManager DocumentManager { get; }

        public IndexManager IndexManager { get; }

        void IDisposable.Dispose()
        {
            ((IDisposable)DocumentManager).Dispose();
            ((IDisposable)IndexManager).Dispose();
        }
    }
}
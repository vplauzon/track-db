using Ipdb.Lib.Document;
using Ipdb.Lib.Indexing;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Ipdb.Lib
{
    internal class DataManager : IDisposable
    {
        private const string DATA_FILE_NAME = "ipdb.data";

        private readonly StorageManager _storageManager;
        private long _nextTransactionId = 0;

        #region Constructors
        public DataManager(
            string databaseRootDirectory,
            IImmutableList<TableIndexKey> tableIndexKeys)
        {
            EnsureDirectory(databaseRootDirectory);
            _storageManager = new(Path.Combine(databaseRootDirectory, DATA_FILE_NAME));
            DocumentManager = new(_storageManager);
            IndexManager = new(_storageManager, tableIndexKeys);
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
            ((IDisposable)_storageManager).Dispose();
        }

        #region Transaction
        public TransactionContext CreateTransaction()
        {
            var transactionId = Interlocked.Increment(ref _nextTransactionId);

            return new TransactionContext(transactionId, this);
        }

        public void CompleteTransaction(long transactionId)
        {
            throw new NotImplementedException();
        }

        public void RollbackTransaction(long transactionId)
        {
            throw new NotImplementedException();
        }
        #endregion
    }
}
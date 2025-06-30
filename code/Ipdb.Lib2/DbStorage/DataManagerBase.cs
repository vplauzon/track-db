using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ipdb.Lib2.DbStorage
{
    internal class DataManagerBase
    {
        protected DataManagerBase(StorageManager storageManager)
        {
            StorageManager = storageManager;
        }

        protected StorageManager StorageManager { get; }
    }
}
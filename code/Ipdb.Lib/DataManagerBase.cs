using Ipdb.Lib.DbStorage;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace Ipdb.Lib
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
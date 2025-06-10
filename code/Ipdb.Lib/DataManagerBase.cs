using Ipdb.Lib.Document;
using Ipdb.Lib.Indexing;
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

        #region Serialization
        protected byte[] Serialize(object document)
        {
            var bufferWriter = new ArrayBufferWriter<byte>();

            using (var writer = new Utf8JsonWriter(bufferWriter))
            {
                JsonSerializer.Serialize(writer, document);
            }

            return bufferWriter.WrittenMemory.ToArray();
        }
        #endregion
    }
}
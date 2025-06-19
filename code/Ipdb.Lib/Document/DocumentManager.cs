using Ipdb.Lib.Cache;
using System;
using System.Buffers;
using System.Threading;

namespace Ipdb.Lib.Document
{
    internal class DocumentManager : DataManagerBase
    {
        public DocumentManager(StorageManager storageManager)
            : base(storageManager)
        {
        }

        //public long AppendDocument(object document)
        //{
        //    if (document == null)
        //    {
        //        throw new ArgumentNullException(nameof(document));
        //    }

        //    var revisionId = Interlocked.Increment(ref _revisionId);
        //    var serializedDocument = Serialize(document);
        //    var blockId = StorageManager.ReserveBlock();

        //    if (serializedDocument.Length * sizeof(byte)
        //        > StorageManager.BlockSize - sizeof(short))
        //    {
        //        throw new ArgumentOutOfRangeException(
        //            nameof(document),
        //            $"Document size:  {serializedDocument.Length}");
        //    }
        //    using (var accessor = StorageManager.CreateViewAccessor(blockId, false))
        //    {
        //        var startOffset = 0;
        //        var offset = startOffset;

        //        accessor.Write(offset, revisionId);
        //        offset += sizeof(long);
        //        accessor.Write(offset, (short)(serializedDocument.Length * sizeof(byte)));
        //        offset += sizeof(short);
        //        accessor.WriteArray(offset, serializedDocument, 0, serializedDocument.Length);
        //        offset += serializedDocument.Length * sizeof(byte);

        //        return revisionId;
        //    }
        //}

        public DatabaseCache PersistDocuments(DatabaseCache cache, bool doPersistEverything)
        {
            throw new NotImplementedException();
        }
    }
}

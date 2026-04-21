using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace TrackDb.Lib
{
    internal class BlockTombstones
    {
        private readonly BitArray _bitmapMask;

        public BlockTombstones(int blockId, string tableName, BitArray bitmapMask)
        {
            BlockId = blockId;
            TableName = tableName;
            _bitmapMask = bitmapMask;
            DeletedCount = ComputePopCount(bitmapMask);
        }

        public int BlockId { get; }

        public string TableName { get; }

        public int ItemCount => _bitmapMask.Count;

        public int DeletedCount { get; }

        private static int ComputePopCount(BitArray bitmapMask)
        {
            int count = 0;
            var bytes = new byte[(bitmapMask.Length + 7) / 8];

            bitmapMask.CopyTo(bytes, 0);
            foreach (byte b in bytes)
            {
                count += System.Numerics.BitOperations.PopCount(b);
            }

            return count;
        }
    }
}
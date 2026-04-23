using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace TrackDb.Lib
{
    internal class BlockTombstones
    {
        private readonly BitArray _bitmapMask;
        private readonly DateTime _lastUpdated = DateTime.Now;

        public BlockTombstones(
            int blockId,
            string tableName,
            BitArray bitmapMask)
        {
            BlockId = blockId;
            TableName = tableName;
            _bitmapMask = bitmapMask;
            DeletedCount = ComputePopCount(bitmapMask);
        }

        public BlockTombstones(
            int blockId,
            string tableName,
            int itemCount,
            IEnumerable<int> rowIndexes)
            : this(blockId, tableName, CreateBitmapMask(itemCount, rowIndexes))
        {
        }

        public int BlockId { get; }

        public string TableName { get; }

        public int ItemCount => _bitmapMask.Count;

        public int DeletedCount { get; }

        public bool IsAllDeleted => DeletedCount == ItemCount;

        public TimeSpan SinceLastUpdated => DateTime.Now - _lastUpdated;

        public bool IsDeleted(int rowIndex)
        {
            return _bitmapMask.Get(rowIndex);
        }

        public BlockTombstones AddRowIndexes(IEnumerable<int> rowIndexes)
        {
            var bitmapMask = new BitArray(_bitmapMask);

            foreach (var rowIndex in rowIndexes)
            {
                bitmapMask.Set(rowIndex, true);
            }

            return new BlockTombstones(BlockId, TableName, bitmapMask);
        }

        public int[] GetTombstoneRowIndexes()
        {
            var rowIndexes = new int[DeletedCount];
            var j = 0;

            for (var i = 0; i != _bitmapMask.Count; ++i)
            {
                if (_bitmapMask.Get(i))
                {
                    rowIndexes[j++] = i;
                }
            }

            return rowIndexes;
        }

        #region Object methods
        public override string ToString()
        {
            return $"'{TableName}' ({BlockId}):  {DeletedCount}/{ItemCount}";
        }
        #endregion

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

        private static BitArray CreateBitmapMask(int itemCount, IEnumerable<int> rowIndexes)
        {
            var bitmapMask = new BitArray(itemCount);

            foreach (var rowIndex in rowIndexes)
            {
                bitmapMask.Set(rowIndex, true);
            }

            return bitmapMask;
        }
    }
}
using Ipdb.Lib.DbStorage;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;

namespace Ipdb.Lib.Cache
{
    internal class IndexBlockCollection
    {
        private readonly ImmutableList<IndexBlock> _blocks = ImmutableList<IndexBlock>.Empty;

        public IEnumerable<IndexBlock> GetIndexBlocks(short indexHash)
        {
            var blockIndex = _blocks.BinarySearch(
                new IndexBlock(0, indexHash, indexHash, 0),
                Comparer<IndexBlock>.Create((a, b) => a.MinHash.CompareTo(b.MinHash)));

            if (blockIndex != -1)
            {
                if (_blocks[blockIndex].MinHash <= indexHash)
                {
                    throw new InvalidDataException("Invalid hash range");
                }
                yield return _blocks[blockIndex];

                while (++blockIndex < _blocks.Count
                    && _blocks[blockIndex].MinHash <= indexHash
                    && _blocks[blockIndex].MaxHash >= indexHash)
                {
                    yield return _blocks[blockIndex];
                }
            }
        }
    }
}
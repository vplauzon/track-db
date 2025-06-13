using System;
using System.Collections.Immutable;
using System.Linq;

namespace Ipdb.Lib.Indexing
{
    internal class IndexBlockCache
    {
        private readonly IImmutableList<IndexBlock> _blocks = ImmutableArray<IndexBlock>.Empty;

        public int? GetBlockId(short indexHash)
        {
            if (_blocks.Any())
            {
            }
            else
            {
                return null;
            }
        }
    }
}
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;

namespace Ipdb.Lib.Cache
{
    internal class DocumentBlockCollection
    {
        private readonly ImmutableList<DocumentBlock> _blocks = ImmutableList<DocumentBlock>.Empty;

        public IEnumerable<IndexBlock> GetDocumentBlocks(short indexHash)
        {
            throw new NotImplementedException();
        }
    }
}
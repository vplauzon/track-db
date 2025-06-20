using Ipdb.Lib.DbStorage;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;

namespace Ipdb.Lib.Cache
{
    internal class DocumentBlockCollection
    {
        private readonly ImmutableList<DocumentBlock> _blocks;

        #region Constructors
        public DocumentBlockCollection()
            :this(ImmutableList<DocumentBlock>.Empty)
        {
        }

        private DocumentBlockCollection(ImmutableList<DocumentBlock> blocks)
        {
            _blocks = blocks;
        }
        #endregion

        public DocumentBlockCollection AddBlock(DocumentBlock block)
        {
            var blockIndex = _blocks.BinarySearch(
                block,
                Comparer<DocumentBlock>.Create(
                    (a, b) => a.MinRevisionId.CompareTo(b.MinRevisionId)));

            throw new NotImplementedException();
        }
 
        public DocumentBlock GetDocumentBlock(long revisionId)
        {
            throw new NotImplementedException();
        }
    }
}
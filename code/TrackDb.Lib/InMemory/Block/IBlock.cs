﻿using TrackDb.Lib.Predicate;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;

namespace TrackDb.Lib.InMemory.Block
{
    internal interface IBlock
    {
        TableSchema TableSchema { get; }

        int RecordCount { get; }

        /// <summary>Filters the rows given a predicate.</summary>
        /// <param name="predicate"></param>
        /// <returns>Filtered row indexes.</returns>
        IEnumerable<int> Filter(IQueryPredicate predicate);

        /// <summary>
        /// Project columns of given row indexes.  See <see cref="TableQuery"/> for column indexes.
        /// The order of rows is guaranteed to be the same as <paramref name="rowIndexes"/>.
        /// The order of columns is guaranteed to be the same as
        /// <paramref name="projectionColumnIndexes"/>.
        /// </summary>
        /// <param name="buffer">Buffer used to carry column values.</param>
        /// <param name="projectionColumnIndexes">Column index to project.</param>
        /// <param name="rowIndexes">Row index to project.</param>
        /// <param name="blockId">Block ID to return if virtual column is projected.</param>
        /// <returns></returns>
        IEnumerable<ReadOnlyMemory<object?>> Project(
            Memory<object?> buffer,
            IEnumerable<int> projectionColumnIndexes,
            IEnumerable<int> rowIndexes,
            int blockId);
    }
}
using System;
using System.Collections.Generic;
using System.Text;

namespace TrackDb.Lib.Predicate
{
    /// <summary>
    /// Marker interface signaling it's implementated with a strongly typed
    /// generic <see cref="InPredicate{T}"/>.
    /// </summary>
    internal interface IInPredicate
    {
        int ColumnIndex { get; }

        QueryPredicate? InverseIsIn();
    }
}